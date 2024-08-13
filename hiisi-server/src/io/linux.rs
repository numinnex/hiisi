use bytes::BytesMut;
use std::collections::{HashMap, VecDeque};
use std::os::fd::{AsRawFd, FromRawFd};
use std::ptr;
use std::rc::Rc;

const RING_SIZE: u32 = 1024;

pub struct IO<C> {
    ring: io_uring::IoUring,
    key_seq: u64,
    submissions: HashMap<u64, Completion<C>>,
    completions: VecDeque<Completion<C>>,
    context: C,
}

impl<C> IO<C> {
    pub fn new(context: C) -> Self {
        Self {
            ring: io_uring::IoUring::new(RING_SIZE).unwrap(),
            key_seq: 0,
            submissions: HashMap::new(),
            completions: VecDeque::new(),
            context,
        }
    }

    pub fn context(&self) -> &C {
        &self.context
    }

    pub fn run_once(&mut self) {
        log::debug!("Running IO loop");
        self.ring.submit().unwrap();
        //println!("Submitted");
        self.flush_submissions();
        self.flush_completions();
    }

    fn flush_submissions(&mut self) {
        log::debug!("Flushing submissions");
        for event in self.ring.completion() {
            let key = event.user_data();
            let result = event.result();
            println!("Event: {:?}", key);
            let mut c = self.submissions.remove(&key).unwrap();
            c.prepare(result);
            self.completions.push_back(c);
        }
    }

    fn flush_completions(&mut self) {
        log::debug!("Flushing completions");
        loop {
            let c = self.completions.pop_front();
            if let Some(c) = c {
                println!("Completion: {:?}", c);
                c.complete(self);
            } else {
                break;
            }
        }
    }

    pub fn accept(
        &mut self,
        server_sock: Rc<socket2::Socket>,
        server_addr: socket2::SockAddr,
        cb: AcceptCallback<C>,
    ) {
        println!("Accepting connection on sockfd {:?}", server_sock);
        let fd = server_sock.as_raw_fd();
        let c = Completion::Accept {
            fd: 0,
            server_sock,
            server_addr: server_addr.clone(),
            cb,
        };
        println!("Post completion");
        let mut addr_len = server_addr.len();
        let addr_storage = server_addr.as_storage();
        let mut addr = unsafe { *(ptr::addr_of!(addr_storage).cast::<libc::sockaddr>()) };
        let key = self.get_key();
        let e = io_uring::opcode::Accept::new(io_uring::types::Fd(fd), &mut addr, &mut addr_len)
            .build()
            .user_data(key);
        println!("Post entry build");
        {
            let mut sq = self.ring.submission();
            match &c {
                Completion::Accept { .. } => { unsafe {
                    sq.push(&e)
                        .expect("Failed to add entry to submission queue, the queue is full.");
                };
                println!("Pushed entry to submission queue");
            },
                _ => {
                    todo!();
                }
            }
        }
        self.enqueue(key, c);
    }

    pub fn close(&mut self, sock: Rc<socket2::Socket>) {
        // Is anything else needed there ?
        log::debug!("Closing sockfd {:?}", sock);
        drop(sock);
    }

    pub fn recv(&mut self, sock: Rc<socket2::Socket>, cb: RecvCallback<C>) {
        log::debug!("Receiving on sockfd {:?}", sock);
        let buf = BytesMut::with_capacity(4096);
        let sock_fd = sock.as_raw_fd();
        let buf_len = buf.capacity();
        let mut c = Completion::Recv { sock, buf, cb };
        let mut_ptr = match &mut c {
            Completion::Recv { buf, .. } => buf.as_mut_ptr(),
            _ => {
                unreachable!();
            }
        };
        let key = self.get_key();
        let e = io_uring::opcode::Recv::new(io_uring::types::Fd(sock_fd), mut_ptr, buf_len as u32)
            .build()
            .user_data(key);
        {
            let mut sq = self.ring.submission();
            match &c {
                Completion::Recv { .. } => unsafe {
                    sq.push(&e).expect("Failed to add entry to sq")
                },
                _ => {
                    todo!();
                }
            }
        }
        self.enqueue(key, c);
    }

    pub fn send(
        &mut self,
        sock: Rc<socket2::Socket>,
        buf: BytesMut,
        n: usize,
        cb: SendCallback<C>,
    ) {
        log::debug!("Sending on sockfd {:?}", sock);
        let fd = sock.as_raw_fd();
        let len = buf.capacity();
        let c = Completion::Send { sock, buf, n, cb };
        let key = self.get_key();
        let ptr = match &c {
            Completion::Send { buf, .. } => buf.as_ptr(),
            _ => {
                unreachable!();
            }
        };
        let entry = io_uring::opcode::Send::new(io_uring::types::Fd(fd), ptr, len as u32)
            .build()
            .user_data(key);
        {
            let mut sq = self.ring.submission();
            match &c {
                Completion::Send { sock, .. } => unsafe {
                    sq.push(&entry).expect("Failed to push op to que");
                },
                _ => {
                    todo!();
                }
            }
        }
        self.enqueue(key, c)
    }

    fn get_key(&mut self) -> u64 {
        let ret = self.key_seq;
        self.key_seq += 1;
        ret
    }

    fn enqueue(&mut self, key: u64, c: Completion<C>) {
        self.submissions.insert(key, c);
    }
}

pub enum Completion<C> {
    Accept {
        fd: i32,
        server_sock: Rc<socket2::Socket>,
        server_addr: socket2::SockAddr,
        cb: AcceptCallback<C>,
    },
    Close,
    Recv {
        sock: Rc<socket2::Socket>,
        buf: BytesMut,
        cb: RecvCallback<C>,
    },
    Send {
        sock: Rc<socket2::Socket>,
        buf: BytesMut,
        n: usize,
        cb: SendCallback<C>,
    },
}

impl<C> std::fmt::Debug for Completion<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Completion::Accept { .. } => write!(f, "Accept"),
            Completion::Close => write!(f, "Close"),
            Completion::Recv { .. } => write!(f, "Recv"),
            Completion::Send { .. } => write!(f, "Send"),
        }
    }
}

impl<C> Completion<C> {
    fn prepare(&mut self, result: i32) {
        match self {
            Completion::Accept { fd, .. } => {
                *fd = result;
            },
            Completion::Close => {
                todo!();
            }
            Completion::Recv { .. } => {}
            Completion::Send { .. } => {}
        }
    }

    fn complete(self, io: &mut IO<C>) {
        match self {
            Completion::Accept {
                fd,
                server_sock,
                server_addr,
                cb,
            } => {
                println!("Before Accept socket connection in complete");
                let sock = unsafe { socket2::Socket::from_raw_fd(fd) };
                let sock_addr = sock.local_addr().unwrap();
                //let (sock, sock_addr) = server_sock.accept().unwrap();
                println!("Accepted socket: {:?}, addr: {:?}", sock, sock_addr);
                cb(io, server_sock, server_addr, Rc::new(sock), sock_addr);
            }
            Completion::Close => {
                todo!();
            }
            Completion::Recv { sock, mut buf, cb } => {
                let n = buf.len();
                unsafe {
                    buf.set_len(n);
                }
                cb(io, sock, &buf[..], n);
            }
            Completion::Send { sock, buf, n, cb } => {
                let n = sock.send(&buf[..n]).unwrap();
                cb(io, sock, n);
            }
        }
    }
}

type ConnectCallback<C> = fn(&mut IO<C>, Rc<socket2::Socket>, socket2::SockAddr);

type AcceptCallback<C> =
    fn(&mut IO<C>, Rc<socket2::Socket>, socket2::SockAddr, Rc<socket2::Socket>, socket2::SockAddr);

type RecvCallback<C> = fn(&mut IO<C>, Rc<socket2::Socket>, &[u8], usize);

type SendCallback<C> = fn(&mut IO<C>, Rc<socket2::Socket>, usize);
