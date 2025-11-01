use anyhow::{bail, Result};
use bytes::Bytes;
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, RwLock};

use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use rml_rtmp::time::RtmpTimestamp;

#[derive(Clone)]
struct StreamBroadcaster {
    video_tx: broadcast::Sender<(Bytes, u32)>, // (data, timestamp)
    audio_tx: broadcast::Sender<(Bytes, u32)>,
    last_video_seq: Arc<RwLock<Option<(Bytes, u32)>>>, // (seq_header, ts)
    last_audio_seq: Arc<RwLock<Option<(Bytes, u32)>>>,
}

impl StreamBroadcaster {
    fn new() -> Self {
        let (video_tx, _) = broadcast::channel(1024);
        let (audio_tx, _) = broadcast::channel(1024);
        Self {
            video_tx,
            audio_tx,
            last_video_seq: Arc::new(RwLock::new(None)),
            last_audio_seq: Arc::new(RwLock::new(None)),
        }
    }
}

/// 전역 스트림 관리자
type StreamRegistry = Arc<RwLock<HashMap<String, StreamBroadcaster>>>;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    let listener = TcpListener::bind("0.0.0.0:1935").await?;
    info!("DJI Neo RTMP server started → rtmp://<PC-IP>:1935/live/mystream");
    info!("Play in OBS: rtmp://<PC-IP>:1935/live/mystream");

    // 스트림 레지스트리 생성 (앱명/스트림키 → 브로드캐스터 매핑)
    let registry: StreamRegistry = Arc::new(RwLock::new(HashMap::new()));

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("Client connected: {}", addr);

        let registry_clone = Arc::clone(&registry);
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, registry_clone).await {
                error!("Client handling error ({}): {e}", addr);
            }
        });
    }
}

async fn handle_client<S>(mut stream: S, registry: StreamRegistry) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // ─────────────────────────────────────────────────────────────
    // 1) RTMP Handshake (서버 역할)
    // ─────────────────────────────────────────────────────────────
    let mut handshake = Handshake::new(PeerType::Server);
    let mut buf = [0u8; 4096];

    let remaining = loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            bail!("Client disconnected during handshake");
        }

        match handshake.process_bytes(&buf[..n])? {
            HandshakeProcessResult::InProgress { response_bytes } => {
                if !response_bytes.is_empty() {
                    stream.write_all(&response_bytes).await?;
                }
            }
            HandshakeProcessResult::Completed {
                response_bytes,
                remaining_bytes,
            } => {
                if !response_bytes.is_empty() {
                    stream.write_all(&response_bytes).await?;
                }
                info!("RTMP handshake completed");
                break remaining_bytes;
            }
        }
    };

    // ─────────────────────────────────────────────────────────────
    // 2) ServerSession 생성 및 초기화
    // ─────────────────────────────────────────────────────────────
    let config = ServerSessionConfig::new();
    let (mut session, init_results) = ServerSession::new(config)?;

    // 초기 OutboundResponse들 전송
    for r in init_results {
        if let ServerSessionResult::OutboundResponse(pkt) = r {
            if !pkt.bytes.is_empty() {
                stream.write_all(&pkt.bytes).await?;
            }
        }
    }

    // 핸드셰이크 후 남은 바이트 처리
    let mut input_buffer = remaining.to_vec();

    // 현재 연결 상태 추적
    let mut is_publisher = false;
    let mut is_player = false;
    let mut current_stream_key = String::new();
    let mut player_stream_id: u32 = 0; // 플레이어의 stream_id 추적
    let mut video_rx: Option<broadcast::Receiver<(Bytes, u32)>> = None;
    let mut audio_rx: Option<broadcast::Receiver<(Bytes, u32)>> = None;

    // ─────────────────────────────────────────────────────────────
    // 3) 메인 루프: 입력 처리 및 이벤트 핸들링
    // ─────────────────────────────────────────────────────────────
    let mut read_buf = [0u8; 4096];

    loop {
        // 먼저 버퍼에 남은 데이터 처리
        if !input_buffer.is_empty() {
            let bytes_to_process = input_buffer.clone();
            input_buffer.clear();

            let results = session.handle_input(&bytes_to_process)?;

            for result in results {
                match result {
                    ServerSessionResult::OutboundResponse(packet) => {
                        stream.write_all(&packet.bytes).await?;
                    }
                    ServerSessionResult::RaisedEvent(event) => {
                        handle_server_event(
                            event,
                            &mut session,
                            &mut stream,
                            &registry,
                            &mut is_publisher,
                            &mut is_player,
                            &mut current_stream_key,
                            &mut player_stream_id,
                            &mut video_rx,
                            &mut audio_rx,
                        )
                        .await?;
                    }
                    ServerSessionResult::UnhandleableMessageReceived(payload) => {
                        warn!("Unhandled message: {:?}", payload);
                    }
                }
            }
        }

        // 플레이어 모드인 경우 브로드캐스트 데이터 전송
        if is_player {
            // VIDEO: drain until Empty (catch-up)
            if let Some(ref mut rx) = video_rx {
                let mut drained = 0usize;
                loop {
                    match rx.try_recv() {
                        Ok((data, timestamp)) => {
                            // inter-frame(=non-keyframe)은 드롭 허용해도 OK
                            let is_keyframe = data.len() >= 1 && (data[0] >> 4) == 1; // FLV FrameType 1 = keyframe
                            let pkt = session.send_video_data(
                                player_stream_id,
                                data,
                                RtmpTimestamp::new(timestamp),
                                !is_keyframe, // keyframe은 false(=드롭 금지), 그 외 true(=드롭 허용)
                            )?;
                            if !pkt.bytes.is_empty() {
                                stream.write_all(&pkt.bytes).await?;
                            }
                            drained += 1;
                            if drained >= 120 { break; } // 과도한 한 번에 전송 방지(2초분 정도 캡)
                        }
                        Err(broadcast::error::TryRecvError::Empty) => break,
                        Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                            // 너무 시끄러우면 debug로
                            log::debug!("Video frames {} skipped", skipped);
                            continue; // 계속 드레인 시도
                        }
                        Err(broadcast::error::TryRecvError::Closed) => {
                            info!("Stream ended");
                            break;
                        }
                    }
                }
            }

            // AUDIO: drain until Empty (보통 드롭 비권장 → can_be_dropped=false 유지)
            if let Some(ref mut rx) = audio_rx {
                let mut drained = 0usize;
                loop {
                    match rx.try_recv() {
                        Ok((data, timestamp)) => {
                            let pkt = session.send_audio_data(
                                player_stream_id,
                                data,
                                RtmpTimestamp::new(timestamp),
                                false, // 오디오는 끊김 티가 커서 드롭 비권장
                            )?;
                            if !pkt.bytes.is_empty() {
                                stream.write_all(&pkt.bytes).await?;
                            }
                            drained += 1;
                            if drained >= 120 { break; }
                        }
                        Err(broadcast::error::TryRecvError::Empty) => break,
                        Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                            log::debug!("Audio frames {} skipped", skipped);
                            continue;
                        }
                        Err(broadcast::error::TryRecvError::Closed) => {
                            info!("Stream ended");
                            break;
                        }
                    }
                }
            }
        }

        // 새 데이터 읽기 (타임아웃 설정)
        let n = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            stream.read(&mut read_buf),
        )
        .await;

        match n {
            Ok(Ok(0)) => {
                info!("Connection Closed");
                break;
            }
            Ok(Ok(n)) => {
                input_buffer.extend_from_slice(&read_buf[..n]);
            }
            Ok(Err(e)) => {
                error!("Read error: {}", e);
                break;
            }
            Err(_) => {
                // Timeout - continue to next loop
                continue;
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_server_event<S>(
    event: ServerSessionEvent,
    session: &mut ServerSession,
    stream: &mut S,
    registry: &StreamRegistry,
    is_publisher: &mut bool,
    is_player: &mut bool,
    current_stream_key: &mut String,
    player_stream_id: &mut u32,
    video_rx: &mut Option<broadcast::Receiver<(Bytes, u32)>>,
    audio_rx: &mut Option<broadcast::Receiver<(Bytes, u32)>>,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    match event {
        ServerSessionEvent::ConnectionRequested { request_id, app_name } => {
            info!("Connection request: app={}", app_name);
            for r in session.accept_request(request_id)? {
                if let ServerSessionResult::OutboundResponse(pkt) = r {
                    if !pkt.bytes.is_empty() {
                        stream.write_all(&pkt.bytes).await?;
                    }
                }
            }
        }

        ServerSessionEvent::PublishStreamRequested { request_id, app_name, stream_key, mode } => {
            info!("Publish request: app={}, key={}, mode={:?}", app_name, stream_key, mode);

            // 스트림 브로드캐스터 생성 또는 가져오기
            let full_key = format!("{}/{}", app_name, stream_key);
            let mut reg = registry.write().await;
            reg.entry(full_key.clone()).or_insert_with(StreamBroadcaster::new);
            drop(reg);

            for r in session.accept_request(request_id)? {
                if let ServerSessionResult::OutboundResponse(pkt) = r {
                    if !pkt.bytes.is_empty() {
                        stream.write_all(&pkt.bytes).await?;
                    }
                }
            }

            *is_publisher = true;
            *current_stream_key = stream_key;
            info!("Publish accepted: {}", full_key);
        }

        // 플레이 요청: stream_id는 반드시 저장해서 이후 전송에 사용
        ServerSessionEvent::PlayStreamRequested { request_id, app_name, stream_key, stream_id, .. } => {
            info!("Play request: app={}, key={}, stream_id={}", app_name, stream_key, stream_id);

            let full_key = format!("{}/{}", app_name, stream_key);
            let reg = registry.read().await;

            if let Some(broadcaster) = reg.get(&full_key) {
                *video_rx = Some(broadcaster.video_tx.subscribe());
                *audio_rx = Some(broadcaster.audio_tx.subscribe());
                drop(reg);

                for r in session.accept_request(request_id)? {
                    if let ServerSessionResult::OutboundResponse(pkt) = r {
                        if !pkt.bytes.is_empty() {
                            stream.write_all(&pkt.bytes).await?;
                        }
                    }
                }

                // 캐시된 시퀀스 헤더를 먼저 전송 (있다면)
                let reg2 = registry.read().await;
                if let Some(broadcaster) = reg2.get(&full_key) {
                    if let Some((vseq, vts)) = broadcaster.last_video_seq.read().await.clone() {
                        let pkt = session.send_video_data(
                            stream_id,
                            vseq,
                            RtmpTimestamp::new(vts),
                            false, // 헤더는 드롭 금지
                        )?;
                        if !pkt.bytes.is_empty() {
                            stream.write_all(&pkt.bytes).await?;
                        }
                    }
                    if let Some((aseq, ats)) = broadcaster.last_audio_seq.read().await.clone() {
                        let pkt = session.send_audio_data(
                            stream_id,
                            aseq,
                            RtmpTimestamp::new(ats),
                            false, // 헤더는 드롭 금지
                        )?;
                        if !pkt.bytes.is_empty() {
                            stream.write_all(&pkt.bytes).await?;
                        }
                    }
                }
                drop(reg2);

                *is_player = true;
                *current_stream_key = stream_key;
                *player_stream_id = stream_id;
                info!("Play accepted: {}", full_key);
            } else {
                drop(reg);
                warn!("Stream not found: {}", full_key);
                // 필요시 거부 로직 추가 가능
            }
        }

        ServerSessionEvent::VideoDataReceived { app_name, stream_key, data, timestamp } => {
            // 퍼블리셔로부터 받은 비디오를 브로드캐스트
            let full_key = format!("{}/{}", app_name, stream_key);
            let reg = registry.read().await;

            if let Some(broadcaster) = reg.get(&full_key) {
                let ts_value = timestamp.value;

                // 브로드캐스트(기존)
                let _ = broadcaster.video_tx.send((data.clone(), ts_value));

                // ── 시퀀스 헤더 감지 (AVC: codec_id=7, AVCPacketType=0)
                if data.len() >= 2 && (data[0] & 0x0F) == 7 && data[1] == 0 {
                    *broadcaster.last_video_seq.write().await = Some((data.clone(), ts_value));
                    // 참고: data.clone()은 Bytes라서 비용 매우 작음
                }
            }
        }

        ServerSessionEvent::AudioDataReceived { app_name, stream_key, data, timestamp } => {
            // 퍼블리셔로부터 받은 오디오를 브로드캐스트
            let full_key = format!("{}/{}", app_name, stream_key);
            let reg = registry.read().await;

            if let Some(broadcaster) = reg.get(&full_key) {
                let ts_value = timestamp.value;

                // 브로드캐스트(기존)
                let _ = broadcaster.audio_tx.send((data.clone(), ts_value));

                // ── 시퀀스 헤더 감지 (AAC: SoundFormat=10, AACPacketType=0)
                if data.len() >= 2 && (data[0] >> 4) == 10 && data[1] == 0 {
                    *broadcaster.last_audio_seq.write().await = Some((data.clone(), ts_value));
                }
            }
        }

        // 메타데이터 이벤트 (이름은 버전에 따라 Changed/Received 중 하나일 수 있음)
        ServerSessionEvent::StreamMetadataChanged { app_name, stream_key, metadata } => {
            info!("Metadata received: app={}, key={}, metadata={:?}", app_name, stream_key, metadata);
        }

        ServerSessionEvent::PublishStreamFinished { app_name, stream_key } => {
            info!("Publish finished: app={}, key={}", app_name, stream_key);
        }

        ServerSessionEvent::PlayStreamFinished { app_name, stream_key } => {
            info!("Playback finished: app={}, key={}", app_name, stream_key);
        }

        // rust-analyzer가 요구한 누락된 암들: 필드 무시로 안전히 처리
        ServerSessionEvent::ClientChunkSizeChanged { .. } => {
            // 필요시 세션/IO의 청크 사이즈 조정 로직 배치
            // (가이드: SetChunkSize 수신 시 즉시 갱신)
            // session.set_max_chunk_size(new_size); // 필드명이 버전에 따라 달라질 수 있어 생략
        }

        ServerSessionEvent::ReleaseStreamRequested { request_id, .. } => {
            // releaseStream 요청은 수락 응답을 보냄
            for r in session.accept_request(request_id)? {
                if let ServerSessionResult::OutboundResponse(pkt) = r {
                    if !pkt.bytes.is_empty() {
                        stream.write_all(&pkt.bytes).await?;
                    }
                }
            }
        }

        ServerSessionEvent::UnhandleableAmf0Command { command_name, .. } => {
            log::debug!("Unhandleable AMF0 command: {}", command_name);
        }

        ServerSessionEvent::AcknowledgementReceived { bytes_received: _ } => {
            // 자동 처리됨
        }

        ServerSessionEvent::PingResponseReceived { timestamp: _ } => {
            // 자동 처리됨
        }
    }

    Ok(())
}
