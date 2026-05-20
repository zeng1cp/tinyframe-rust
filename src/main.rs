// use std::io::{self, Read, Write};
// use std::net::TcpStream;
// use std::time::Duration;
// use tinyframe::*;
// use std::sync::Arc;
// use std::sync::atomic::{AtomicBool, Ordering};
// use std::sync::mpsc::{self, TryRecvError};
// use std::thread;


// fn drain_responses(rx: &mpsc::Receiver<String>) {
//     loop {
//         match rx.try_recv() {
//             Ok(msg) => println!("[System] {}", msg),
//             Err(TryRecvError::Empty) => break,
//             Err(TryRecvError::Disconnected) => break,
//         }
//     }
// }

// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     // 创建退出标志
//     let stop_flag = Arc::new(AtomicBool::new(false));

//     // 创建通道：主线程 -> 收发线程（发送用户数据）
//     let (tx_to_comm, rx_from_comm) = mpsc::channel();
//     // 创建通道：收发线程 -> 主线程（发送状态和响应）
//     let (tx_to_main, rx_from_main) = mpsc::channel();

//     let stop_flag_clone = stop_flag.clone();
//     let comm_thread = thread::spawn(move || {
//         communication_thread(stop_flag_clone, tx_to_main, rx_from_comm);
//     });

//     let mut input = String::new();
//     loop {
//         // 先打印所有已收到的响应（来自上一次发送）
//         drain_responses(&rx_from_main);

//         print!("Enter message (type 'quit' to exit): ");
//         io::stdout().flush()?;
//         input.clear();
//         if io::stdin().read_line(&mut input)? == 0 { break; }
//         let msg = input.trim();
//         if msg == "quit" { break; }

//         tx_to_comm.send(msg.as_bytes().to_vec())?;
//         // 可选：短暂等待一下可能的即时响应
//         thread::sleep(Duration::from_millis(50));
//         drain_responses(&rx_from_main);
//     }

//     // 通知收发线程退出
//     stop_flag.store(true, Ordering::Relaxed);
//     // 等待线程结束（可选）
//     comm_thread.join().unwrap();
//     Ok(())
// }

// const ID_SIZE:usize = 1;
// const LN_SIZE:usize = 4;
// const TY_SIZE:usize = 2;

// fn communication_thread(
//     stop_flag: Arc<AtomicBool>,
//     tx_to_main: mpsc::Sender<String>,
//     rx_from_main: mpsc::Receiver<Vec<u8>>,
// ) {
//     // 外层循环：负责重连
//     while !stop_flag.load(Ordering::Relaxed) {
//         // 尝试连接服务器
//         let stream = match TcpStream::connect("127.0.0.1:8888") {
//             Ok(s) => s,
//             Err(e) => {
//                 let _ = tx_to_main.send(format!("Connection failed: {}, retrying in 5s", e));
//                 thread::sleep(Duration::from_secs(5));
//                 continue;
//             }
//         };
//         let _ = tx_to_main.send("Connected to server".to_string());

//         // 准备传输层
//         let transport = SocketTransport(stream.try_clone().unwrap());
//         let mut read_stream = stream.try_clone().unwrap();
//         read_stream.set_nonblocking(true).unwrap(); // 非阻塞，便于同时检查通道

//         // 创建上下文，携带发送端
//         let ctx = Ctx::new(tx_to_main.clone());

//         // 创建 TinyFrame 实例
//         let mut tf = TinyFrame::<_, _, _, 1024, 5, 5, ID_SIZE, LN_SIZE, TY_SIZE>::new(
//             ctx,
//             transport,
//             Crc16,
//             Peer::Master,
//             0xAA,
//             10,
//         )
//         .unwrap();
//         tf.add_type_listener(0x22, response_listener).unwrap();

//         let mut buf = [0; 1024];
//         // 内部循环：处理收发，直到连接断开或收到退出信号
//         'inner: loop {
//             if stop_flag.load(Ordering::Relaxed) {
//                 break 'inner;
//             }

//             // 检查是否有消息需要发送（来自主线程）
//             match rx_from_main.try_recv() {
//                 Ok(data) => {
//                     let frame = Frame {
//                         id: 0,
//                         typ: 0x22,
//                         is_response: false,
//                     };
//                     if let Err(e) = tf.send(frame, &data) {
//                         let _ = tx_to_main.send(format!("Send error: {:?}, reconnecting", e));
//                         break 'inner; // 发送失败，重连
//                     }
//                 }
//                 Err(TryRecvError::Empty) => {}
//                 Err(TryRecvError::Disconnected) => {
//                     // 主线程已关闭，退出整个线程
//                     return;
//                 }
//             }

//             // 检查 socket 是否有数据可读
//             match read_stream.read(&mut buf) {
//                 Ok(0) => {
//                     let _ = tx_to_main.send("Server closed connection".to_string());
//                     break 'inner;
//                 }
//                 Ok(n) => {
//                     tf.accept(&buf[..n]);
//                 }
//                 Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
//                     // 无数据，继续
//                 }
//                 Err(e) => {
//                     let _ = tx_to_main.send(format!("Read error: {}, reconnecting", e));
//                     break 'inner;
//                 }
//             }

//             tf.tick();
//             // 短暂休眠避免忙循环
//             thread::sleep(Duration::from_millis(10));
//         }
//     }
// }

// // 传输层实现
// struct SocketTransport(TcpStream);

// impl Transport for SocketTransport {
//     type Error = std::io::Error;

//     fn write(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
//         self.0.write_all(bytes)
//     }
// }

// // 用户上下文（本例中仅用于演示）
// struct Ctx {
//     message_count: usize,
//     tx: mpsc::Sender<String>, // 向主线程发送消息的通道
// }
// impl Ctx {
//     fn new(tx: mpsc::Sender<String>) -> Self {
//         Self {
//             message_count: 0,
//             tx,
//         }
//     }
// }

// fn response_listener(
//     ctx: &mut Ctx,
//     _channel: &mut FrameChannel<'_, SocketTransport, Crc16, ID_SIZE, LN_SIZE, TY_SIZE>,
//     frame: ReceivedFrame<'_>,
// ) -> ListenerAction {
//     ctx.message_count += 1;
//     let text = format!(
//         "[{}] Received response: id={}, type=0x02, data={:?}",
//         ctx.message_count,
//         frame.id,
//         std::str::from_utf8(frame.data).unwrap_or("<binary>")
//     );
//     // 发送给主线程，忽略错误（主线程可能已关闭）
//     let _ = ctx.tx.send(text);
//     ListenerAction::Stay
// }

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use tinyframe::*; // 假设库名为 tinyframe_rust，请根据实际调整

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
    let mut read_stream = stream.try_clone()?;
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