use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use tinyframe_rust::*; // 假设库名为 tinyframe_rust，请根据实际调整

const SERVER_ADDR: &str = "127.0.0.1:8888";
const SOF: u8 = 0xAA;
const ID_BYTES: usize = 1;
const LEN_BYTES: usize = 1;
const TYPE_BYTES: usize = 1;

/// 传输层包装，实现 Transport trait
struct SocketTransport(TcpStream);

impl Transport for SocketTransport {
    type Error = io::Error;

    fn write(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        // 打印发送的字节用于调试
        println!("[Server TX] {}", hex::encode(bytes));
        self.0.write_all(bytes)
    }
}

/// 用户上下文，用于存储统计信息（可选）
#[derive(Default)]
struct Ctx {
    message_count: usize,
}

/// 类型监听器：0x22 回显
fn echo_listener(
    ctx: &mut Ctx,
    channel: &mut FrameChannel<'_, SocketTransport, NoChecksum, ID_BYTES, LEN_BYTES, TYPE_BYTES>,
    frame: ReceivedFrame<'_>,
) -> ListenerAction {
    ctx.message_count += 1;
    println!(
        "[Server] Echo request #{}: id={}, data={:?}",
        ctx.message_count,
        frame.id,
        std::str::from_utf8(frame.data).unwrap_or("<binary>")
    );

    // 原样返回
    let resp_frame = Frame {
        id: frame.id,
        typ: frame.typ,
        is_response: true,
    };
    if let Err(e) = channel.send(resp_frame, frame.data) {
        eprintln!("发送回显响应失败: {:?}", e);
    }
    ListenerAction::Stay
}

/// 类型监听器：0x23 返回 "pong"
fn query_listener(
    ctx: &mut Ctx,
    channel: &mut FrameChannel<'_, SocketTransport, NoChecksum, ID_BYTES, LEN_BYTES, TYPE_BYTES>,
    frame: ReceivedFrame<'_>,
) -> ListenerAction {
    ctx.message_count += 1;
    println!(
        "[Server] Query request #{}: id={}, responding with pong",
        ctx.message_count, frame.id
    );

    let resp_frame = Frame {
        id: frame.id,
        typ: frame.typ,
        is_response: true,
    };
    if let Err(e) = channel.send(resp_frame, b"pong") {
        eprintln!("发送查询响应失败: {:?}", e);
    }
    ListenerAction::Stay
}

/// 类型监听器：0x24 多部分帧确认
fn multipart_listener(
    ctx: &mut Ctx,
    channel: &mut FrameChannel<'_, SocketTransport, NoChecksum, ID_BYTES, LEN_BYTES, TYPE_BYTES>,
    frame: ReceivedFrame<'_>,
) -> ListenerAction {
    ctx.message_count += 1;
    println!(
        "[Server] Multipart received #{}: id={}, total {} bytes",
        ctx.message_count,
        frame.id,
        frame.data.len()
    );

    let resp_frame = Frame {
        id: frame.id,
        typ: frame.typ,
        is_response: true,
    };
    if let Err(e) = channel.send(resp_frame, b"OK") {
        eprintln!("发送多部分确认失败: {:?}", e);
    }
    ListenerAction::Stay
}

/// 处理单个客户端连接
fn handle_client(stream: TcpStream) -> io::Result<()> {
    let peer_addr = stream.peer_addr()?;
    println!("[Server] Client connected: {}", peer_addr);

    // 克隆流用于读取和写入（TcpStream 可以 clone）
    let read_stream = stream.try_clone()?;
    let transport = SocketTransport(stream);

    // 创建 TinyFrame 实例，无校验和
    let mut tf = TinyFrame::<_, _, _, 1024, 5, 5, ID_BYTES, LEN_BYTES, TYPE_BYTES>::new(
        Ctx::default(),
        transport,
        NoChecksum,
        Peer::Slave,   // 服务器作为 Slave
        SOF,
        10,            // parser timeout ticks
    )
    .expect("创建 TinyFrame 失败");

    // 注册类型监听器
    tf.add_type_listener(0x22, echo_listener)
        .expect("注册 echo 监听器失败");
    tf.add_type_listener(0x23, query_listener)
        .expect("注册 query 监听器失败");
    tf.add_type_listener(0x24, multipart_listener)
        .expect("注册 multipart 监听器失败");
    // 类型 0xFF 故意不注册，用于超时测试

    // 设置非阻塞读取，以便在空闲时也能调用 tick
    read_stream.set_nonblocking(true)?;
    let mut buf = [0; 1024];

    loop {
        // 读取数据
        match read_stream.read(&mut buf) {
            Ok(0) => {
                println!("[Server] Client {} disconnected", peer_addr);
                break;
            }
            Ok(n) => {
                let data = &buf[..n];
                println!("[Server RX] {}", hex::encode(data));
                tf.accept(data);
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // 无数据，稍后继续
            }
            Err(e) => {
                eprintln!("[Server] Read error from {}: {}", peer_addr, e);
                break;
            }
        }

        // 调用 tick 处理超时
        tf.tick();

        // 短暂休眠避免忙循环
        thread::sleep(std::time::Duration::from_millis(10));
    }

    Ok(())
}

fn main() -> io::Result<()> {
    let listener = TcpListener::bind(SERVER_ADDR)?;
    println!("Rust TinyFrame server listening on {}", SERVER_ADDR);
    println!("SOF=0x{:02X}, fields=1 byte, checksum=None", SOF);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| {
                    if let Err(e) = handle_client(stream) {
                        eprintln!("处理客户端时出错: {}", e);
                    }
                });
            }
            Err(e) => eprintln!("接受连接失败: {}", e),
        }
    }

    Ok(())
}