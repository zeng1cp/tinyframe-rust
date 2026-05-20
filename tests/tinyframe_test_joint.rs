use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;
use std::thread;

const SERVER_ADDR: &str = "127.0.0.1:8888";
const SOF: u8 = 0xAA; // 必须与 Kotlin 服务器配置一致

/// 日志记录函数，同时输出到控制台和文件
fn log_message(msg: &str) {
    // 测试中允许直接打印，cargo test 会捕获输出
    println!("{}", msg);
    // 如果需要写入文件，可取消注释
    let mut file = OpenOptions::new()
        .create(true)
    .append(true)
    .open("test_log.txt")
    .expect("无法打开日志文件");
    writeln!(file, "{}", msg).expect("写入日志失败");
}

/// 测试客户端封装
struct TestClient {
    stream: TcpStream,
    test_name: String,
}

impl TestClient {
    /// 连接服务器并初始化
    fn connect(test_name: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(SERVER_ADDR)?;
        Ok(TestClient {
            stream,
            test_name: test_name.to_string(),
        })
    }

    /// 发送原始字节帧
    fn send(&mut self, frame: &[u8]) {
        log_message(&format!("[{}] TX: {}", self.test_name, hex::encode(frame)));
        self.stream.write_all(frame).expect("发送失败");
    }

    /// 接收响应（可选超时）
    fn receive(&mut self, timeout: Option<Duration>) -> Option<Vec<u8>> {
        self.stream.set_read_timeout(timeout).expect("设置超时失败");
        let mut buf = [0; 4096];
        match self.stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                let data = buf[..n].to_vec();
                log_message(&format!("[{}] RX: {}", self.test_name, hex::encode(&data)));
                Some(data)
            }
            Ok(_) => None,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => None,
            Err(e) => panic!("读取错误: {}", e),
        }
    }

    /// 构建标准 TinyFrame 帧（无校验和）
    fn build_frame(&self, id: u8, typ: u8, data: &[u8]) -> Vec<u8> {
        let mut frame = Vec::with_capacity(4 + data.len());
        frame.push(SOF);
        frame.push(id);
        frame.push(data.len() as u8);
        frame.push(typ);
        frame.extend_from_slice(data);
        frame
    }

    /// 解析接收到的帧（假设格式正确）
    fn parse_frame<'a>(&self, frame: &'a [u8]) -> (u8, u8, &'a [u8]) {
        assert!(frame.len() >= 4 && frame[0] == SOF, "无效帧");
        let id = frame[1];
        let len = frame[2] as usize;
        let typ = frame[3];
        let data = &frame[4..4 + len];
        (id, typ, data)
    }
}

/// 测试1：回应功能
#[test]
fn test_echo() {
    let mut client = TestClient::connect("echo").expect("连接失败");
    let data = b"Hello, echo!";
    let frame = client.build_frame(0x01, 0x22, data);
    client.send(&frame);

    if let Some(resp) = client.receive(Some(Duration::from_secs(2))) {
        let (id, typ, resp_data) = client.parse_frame(&resp);
        assert_eq!(id, 0x01);
        assert_eq!(typ, 0x22);
        assert_eq!(resp_data, data);
        log_message("[echo] ✅ 通过");
    } else {
        panic!("未收到响应");
    }
}

/// 测试2：ID 监听器 / query
#[test]
fn test_query() {
    let mut client = TestClient::connect("query").expect("连接失败");
    let data = b"query";
    let frame = client.build_frame(0x02, 0x23, data);
    client.send(&frame);

    if let Some(resp) = client.receive(Some(Duration::from_secs(2))) {
        let (id, typ, resp_data) = client.parse_frame(&resp);
        assert_eq!(id, 0x02);
        assert_eq!(typ, 0x23);
        assert_eq!(resp_data, b"pong");
        log_message("[query] ✅ 通过");
    } else {
        panic!("未收到响应");
    }
}

/// 测试3：多部分传输
#[test]
fn test_multipart() {
    let mut client = TestClient::connect("multipart").expect("连接失败");
    let total_len = 5;
    let id = 0x03;
    let typ = 0x24;

    // 发送头部
    let header = vec![SOF, id, total_len, typ];
    client.send(&header);
    thread::sleep(Duration::from_millis(50));

    // 发送数据块
    client.send(&[1, 2]);
    thread::sleep(Duration::from_millis(50));
    client.send(&[3, 4, 5]);

    // 等待服务器确认
    if let Some(resp) = client.receive(Some(Duration::from_secs(2))) {
        let (_, _, resp_data) = client.parse_frame(&resp);
        assert_eq!(resp_data, b"OK");
        log_message("[multipart] ✅ 通过");
    } else {
        panic!("未收到多部分确认");
    }
}

/// 测试4：超时（发送无响应的帧）
#[test]
fn test_timeout() {
    let mut client = TestClient::connect("timeout").expect("连接失败");
    let data = b"no reply";
    let frame = client.build_frame(0x04, 0xFF, data); // 类型 0xFF 无监听器
    client.send(&frame);

    let resp = client.receive(Some(Duration::from_secs(1)));
    assert!(resp.is_none(), "不应收到响应");
    log_message("[timeout] ✅ 通过");
}