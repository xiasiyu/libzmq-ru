use std::collections::VecDeque;

use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipeFrame<T> {
    Item(T),
    Delimiter,
}

#[derive(Debug, Default)]
pub struct YQueue<T> {
    items: VecDeque<T>,
}

impl<T> YQueue<T> {
    pub fn new() -> Self {
        Self {
            items: VecDeque::new(),
        }
    }

    pub fn push_back(&mut self, item: T) {
        self.items.push_back(item);
    }

    pub fn pop_front(&mut self) -> Option<T> {
        self.items.pop_front()
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[derive(Debug)]
pub struct YPipe<T> {
    queue: YQueue<T>,
    hwm: usize,
    conflate: bool,
    terminated: bool,
}

impl<T> YPipe<T> {
    pub fn new(hwm: usize) -> Self {
        Self {
            queue: YQueue::new(),
            hwm,
            conflate: false,
            terminated: false,
        }
    }

    pub fn set_conflate(&mut self, conflate: bool) {
        self.conflate = conflate;
    }

    pub fn write(&mut self, item: T) -> Result<()> {
        if self.terminated {
            return Err(Error::InvalidSocket);
        }
        if self.conflate {
            self.queue.clear();
        } else if self.hwm > 0 && self.queue.len() >= self.hwm {
            return Err(Error::Again);
        }
        self.queue.push_back(item);
        Ok(())
    }

    pub fn read(&mut self) -> Option<T> {
        self.queue.pop_front()
    }

    pub fn terminate(&mut self) {
        self.terminated = true;
        self.queue.clear();
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

impl<T> YPipe<PipeFrame<T>> {
    pub fn write_item(&mut self, item: T) -> Result<()> {
        self.write(PipeFrame::Item(item))
    }

    pub fn write_delimiter(&mut self) -> Result<()> {
        if self.terminated {
            return Err(Error::InvalidSocket);
        }
        self.queue.push_back(PipeFrame::Delimiter);
        self.terminated = true;
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct Mailbox<T> {
    commands: VecDeque<T>,
}

impl<T> Mailbox<T> {
    pub fn new() -> Self {
        Self {
            commands: VecDeque::new(),
        }
    }

    pub fn send(&mut self, command: T) {
        self.commands.push_back(command);
    }

    pub fn recv(&mut self) -> Option<T> {
        self.commands.pop_front()
    }

    pub fn len(&self) -> usize {
        self.commands.len()
    }

    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{Mailbox, PipeFrame, YPipe, YQueue};
    use crate::Error;

    #[test]
    fn yqueue_preserves_fifo_order() {
        let mut queue = YQueue::new();
        queue.push_back(1);
        queue.push_back(2);

        assert_eq!(queue.len(), 2);
        assert_eq!(queue.pop_front(), Some(1));
        assert_eq!(queue.pop_front(), Some(2));
        assert_eq!(queue.pop_front(), None);
        assert!(queue.is_empty());
    }

    #[test]
    fn ypipe_enforces_hwm() {
        let mut pipe = YPipe::new(1);

        pipe.write("first").unwrap();

        assert_eq!(pipe.write("second"), Err(Error::Again));
        assert_eq!(pipe.read(), Some("first"));
        assert_eq!(pipe.read(), None);
    }

    #[test]
    fn ypipe_conflate_keeps_latest_item() {
        let mut pipe = YPipe::new(1);
        pipe.set_conflate(true);

        pipe.write("first").unwrap();
        pipe.write("second").unwrap();

        assert_eq!(pipe.len(), 1);
        assert_eq!(pipe.read(), Some("second"));
    }

    #[test]
    fn ypipe_termination_drops_pending_items() {
        let mut pipe = YPipe::new(0);
        pipe.write("pending").unwrap();

        pipe.terminate();

        assert!(pipe.is_empty());
        assert_eq!(pipe.write("late"), Err(Error::InvalidSocket));
    }

    #[test]
    fn ypipe_delimiter_marks_end_without_dropping_prior_items() {
        let mut pipe = YPipe::new(0);
        pipe.write_item("pending").unwrap();
        pipe.write_delimiter().unwrap();

        assert_eq!(pipe.write_item("late"), Err(Error::InvalidSocket));
        assert_eq!(pipe.read(), Some(PipeFrame::Item("pending")));
        assert_eq!(pipe.read(), Some(PipeFrame::Delimiter));
        assert_eq!(pipe.read(), None);
    }

    #[test]
    fn mailbox_preserves_command_order() {
        let mut mailbox = Mailbox::new();
        mailbox.send("stop");
        mailbox.send("term");

        assert_eq!(mailbox.len(), 2);
        assert_eq!(mailbox.recv(), Some("stop"));
        assert_eq!(mailbox.recv(), Some("term"));
        assert!(mailbox.is_empty());
    }

    #[test]
    fn loom_models_pipe_shutdown_race() {
        loom::model(|| {
            let pipe = loom::sync::Arc::new(loom::sync::Mutex::new(YPipe::new(0)));
            let writer_pipe = loom::sync::Arc::clone(&pipe);
            let terminator_pipe = loom::sync::Arc::clone(&pipe);

            let writer = loom::thread::spawn(move || {
                let _ = writer_pipe.lock().unwrap().write("message");
            });
            let terminator = loom::thread::spawn(move || {
                terminator_pipe.lock().unwrap().terminate();
            });

            writer.join().unwrap();
            terminator.join().unwrap();

            let mut pipe = pipe.lock().unwrap();
            assert_eq!(pipe.read(), None);
            assert_eq!(pipe.write("late"), Err(Error::InvalidSocket));
        });
    }

    #[test]
    fn loom_models_mailbox_shutdown_command_order() {
        loom::model(|| {
            let mailbox = loom::sync::Arc::new(loom::sync::Mutex::new(Mailbox::new()));
            let first_mailbox = loom::sync::Arc::clone(&mailbox);
            let second_mailbox = loom::sync::Arc::clone(&mailbox);

            let first = loom::thread::spawn(move || {
                first_mailbox.lock().unwrap().send("stop");
            });
            let second = loom::thread::spawn(move || {
                second_mailbox.lock().unwrap().send("term");
            });

            first.join().unwrap();
            second.join().unwrap();

            let mut mailbox = mailbox.lock().unwrap();
            assert_eq!(mailbox.len(), 2);
            assert!(matches!(mailbox.recv(), Some("stop" | "term")));
            assert!(matches!(mailbox.recv(), Some("stop" | "term")));
            assert_eq!(mailbox.recv(), None);
        });
    }
}
