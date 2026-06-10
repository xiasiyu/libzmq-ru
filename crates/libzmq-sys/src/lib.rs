//! Platform and third-party FFI isolation for libzmq.
//!
//! Business logic must not call OS or third-party C APIs directly. Future socket,
//! poller, GSSAPI, OpenPGM, and NORM bindings belong in this crate or modules
//! below it.

pub mod platform {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Family {
        Unix,
        Windows,
    }

    pub fn family() -> Family {
        if cfg!(windows) {
            Family::Windows
        } else {
            Family::Unix
        }
    }
}

#[cfg(feature = "norm")]
pub mod norm {
    use std::ffi::{c_char, c_int, c_uchar, c_uint, c_ushort, c_void, CString};
    use std::ptr;
    use std::time::{Duration, Instant};

    type NormInstanceHandle = *const c_void;
    type NormSessionHandle = *const c_void;
    type NormNodeHandle = *const c_void;
    type NormObjectHandle = *const c_void;
    type NormNodeId = u32;
    type NormSessionId = c_ushort;
    type NormSize = i64;

    const NORM_EVENT_RX_OBJECT_COMPLETED: c_int = 20;
    const NORM_OBJECT_DATA: c_int = 1;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Version {
        pub major: i32,
        pub minor: i32,
        pub patch: i32,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SessionConfig {
        pub address: String,
        pub port: u16,
        pub local_node_id: u32,
        pub buffer_space: u32,
        pub segment_size: u16,
        pub block_size: u16,
        pub parity_segments: u16,
    }

    impl SessionConfig {
        pub fn new(address: impl Into<String>, port: u16, local_node_id: u32) -> Self {
            Self {
                address: address.into(),
                port,
                local_node_id,
                buffer_space: 2 * 1024 * 1024,
                segment_size: 1400,
                block_size: 16,
                parity_segments: 4,
            }
        }
    }

    #[derive(Debug)]
    pub struct Instance {
        raw: NormInstanceHandle,
    }

    #[derive(Debug)]
    pub struct Session {
        raw: NormSessionHandle,
        sender_started: bool,
        receiver_started: bool,
    }

    // SAFETY: NORM handles are opaque library-managed handles. The core stores them behind a
    // mutex, and wrapper methods require `&mut self` or external locking for state mutation.
    unsafe impl Send for Instance {}
    // SAFETY: NORM session handles are opaque library-managed handles. The core stores sessions
    // behind a mutex and does not alias mutable access across threads.
    unsafe impl Send for Session {}

    #[repr(C)]
    struct NormEvent {
        event_type: c_int,
        session: NormSessionHandle,
        sender: NormNodeHandle,
        object: NormObjectHandle,
    }

    #[link(name = "norm")]
    unsafe extern "C" {
        fn NormGetVersion(major: *mut c_int, minor: *mut c_int, patch: *mut c_int) -> c_int;
        fn NormCreateInstance(priority_boost: bool) -> NormInstanceHandle;
        fn NormDestroyInstance(instance: NormInstanceHandle);
        fn NormCreateSession(
            instance: NormInstanceHandle,
            session_address: *const c_char,
            session_port: c_ushort,
            local_node_id: NormNodeId,
        ) -> NormSessionHandle;
        fn NormDestroySession(session: NormSessionHandle);
        fn NormSetTxOnly(
            session: NormSessionHandle,
            tx_only: bool,
            connect_to_session_address: bool,
        );
        fn NormGetRandomSessionId() -> NormSessionId;
        fn NormStartSender(
            session: NormSessionHandle,
            instance_id: NormSessionId,
            buffer_space: u32,
            segment_size: c_ushort,
            block_size: c_ushort,
            num_parity: c_ushort,
            fec_id: c_uchar,
        ) -> bool;
        fn NormStopSender(session: NormSessionHandle);
        fn NormDataEnqueue(
            session: NormSessionHandle,
            data_ptr: *const c_char,
            data_len: u32,
            info_ptr: *const c_char,
            info_len: c_uint,
        ) -> NormObjectHandle;
        fn NormStartReceiver(session: NormSessionHandle, buffer_space: u32) -> bool;
        fn NormStopReceiver(session: NormSessionHandle);
        fn NormGetNextEvent(
            instance: NormInstanceHandle,
            event: *mut NormEvent,
            wait_for_event: bool,
        ) -> bool;
        fn NormObjectGetType(object: NormObjectHandle) -> c_int;
        fn NormObjectGetSize(object: NormObjectHandle) -> NormSize;
        fn NormDataAccessData(object: NormObjectHandle) -> *const c_char;
    }

    pub fn version() -> Option<Version> {
        let mut major = 0;
        let mut minor = 0;
        let mut patch = 0;
        // SAFETY: The three output pointers reference valid writable `c_int` stack slots.
        let _ = unsafe { NormGetVersion(&mut major, &mut minor, &mut patch) };
        (major > 0).then_some(Version {
            major,
            minor,
            patch,
        })
    }

    impl Instance {
        pub fn new() -> Option<Self> {
            // SAFETY: This calls the process-local NORM constructor with no borrowed inputs.
            let raw = unsafe { NormCreateInstance(false) };
            (!raw.is_null()).then_some(Self { raw })
        }

        pub fn create_session(&self, config: &SessionConfig) -> Option<Session> {
            let address = CString::new(config.address.as_str()).ok()?;
            // SAFETY: `self.raw` is a live instance and `address` is a NUL-terminated string valid
            // for the duration of this call. Other arguments are plain scalar values.
            let raw = unsafe {
                NormCreateSession(
                    self.raw,
                    address.as_ptr(),
                    config.port,
                    config.local_node_id,
                )
            };
            (!raw.is_null()).then_some(Session {
                raw,
                sender_started: false,
                receiver_started: false,
            })
        }

        pub fn recv_data(&self, timeout: Duration) -> Option<Vec<u8>> {
            let deadline = Instant::now() + timeout;
            while Instant::now() < deadline {
                let mut event = NormEvent {
                    event_type: 0,
                    session: ptr::null(),
                    sender: ptr::null(),
                    object: ptr::null(),
                };
                // SAFETY: `self.raw` is a live instance and `event` is a valid writable event slot.
                let ready = unsafe { NormGetNextEvent(self.raw, &mut event, false) };
                if ready
                    && event.event_type == NORM_EVENT_RX_OBJECT_COMPLETED
                    && !event.object.is_null()
                    // SAFETY: The event came from NORM and carries a live object handle.
                    && unsafe { NormObjectGetType(event.object) } == NORM_OBJECT_DATA
                {
                    // SAFETY: The event object is completed, so size and data pointer are valid
                    // until we release the object below.
                    let bytes = unsafe {
                        let size = NormObjectGetSize(event.object);
                        let data = NormDataAccessData(event.object);
                        if size <= 0 || data.is_null() {
                            Vec::new()
                        } else {
                            std::slice::from_raw_parts(data.cast::<u8>(), size as usize).to_vec()
                        }
                    };
                    return Some(bytes);
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            None
        }
    }

    impl Drop for Instance {
        fn drop(&mut self) {
            if !self.raw.is_null() {
                // SAFETY: `self.raw` is owned by this wrapper and destroyed exactly once here.
                unsafe { NormDestroyInstance(self.raw) };
                self.raw = ptr::null();
            }
        }
    }

    impl Session {
        pub fn start_receiver(&mut self, buffer_space: u32) -> Option<()> {
            // SAFETY: `self.raw` is a live session handle and the buffer size is a scalar value.
            let ok = unsafe { NormStartReceiver(self.raw, buffer_space) };
            self.receiver_started = ok;
            ok.then_some(())
        }

        pub fn start_sender(&mut self, config: &SessionConfig) -> Option<()> {
            // SAFETY: `self.raw` is a live session handle. Tx-only mode prevents a unicast sender
            // from binding the receiver's session port during local sender/receiver tests.
            unsafe { NormSetTxOnly(self.raw, true, true) };
            // SAFETY: `self.raw` is a live session handle and all sender parameters are scalar
            // values copied from validated Rust configuration.
            let ok = unsafe {
                NormStartSender(
                    self.raw,
                    NormGetRandomSessionId(),
                    config.buffer_space,
                    config.segment_size,
                    config.block_size,
                    config.parity_segments,
                    0,
                )
            };
            self.sender_started = ok;
            ok.then_some(())
        }

        pub fn send_data(&self, payload: &[u8]) -> Option<()> {
            // SAFETY: `self.raw` is a live sender session, and `payload` points to initialized
            // bytes valid for the duration of the call. No object info buffer is supplied.
            let object = unsafe {
                NormDataEnqueue(
                    self.raw,
                    payload.as_ptr().cast::<c_char>(),
                    payload.len().try_into().ok()?,
                    ptr::null(),
                    0,
                )
            };
            (!object.is_null()).then_some(())
        }
    }

    impl Drop for Session {
        fn drop(&mut self) {
            if !self.raw.is_null() {
                if self.sender_started {
                    // SAFETY: Sender was started on this live session and is stopped once here.
                    unsafe { NormStopSender(self.raw) };
                }
                if self.receiver_started {
                    // SAFETY: Receiver was started on this live session and is stopped once here.
                    unsafe { NormStopReceiver(self.raw) };
                }
                // SAFETY: `self.raw` is owned by this wrapper and destroyed exactly once here.
                unsafe { NormDestroySession(self.raw) };
                self.raw = ptr::null();
            }
        }
    }
}

#[cfg(feature = "gssapi")]
pub mod gssapi {
    use std::ffi::{c_int, c_void};
    use std::ptr;

    type OmUint32 = u32;
    type GssName = *mut c_void;
    type GssCred = *mut c_void;
    type GssCtx = *mut c_void;
    type GssOidSet = *mut c_void;
    type GssChannelBindings = *mut c_void;

    const GSS_S_COMPLETE: OmUint32 = 0;
    const GSS_S_CONTINUE_NEEDED: OmUint32 = 1;
    const GSS_C_BOTH: c_int = 0;
    const GSS_C_MUTUAL_FLAG: OmUint32 = 2;
    const GSS_C_REPLAY_FLAG: OmUint32 = 4;
    const GSS_C_QOP_DEFAULT: OmUint32 = 0;

    const OID_HOSTBASED_SERVICE: &[u8] = b"\x2a\x86\x48\x86\xf7\x12\x01\x02\x01\x04";
    const OID_USER_NAME: &[u8] = b"\x2a\x86\x48\x86\xf7\x12\x01\x02\x01\x01";
    const OID_KRB5_PRINCIPAL: &[u8] = b"\x2a\x86\x48\x86\xf7\x12\x01\x02\x02\x01";

    #[repr(C)]
    struct GssBuffer {
        length: usize,
        value: *mut c_void,
    }

    #[repr(C)]
    struct GssOidDesc {
        length: OmUint32,
        elements: *mut c_void,
    }

    unsafe extern "C" {
        fn gss_import_name(
            minor_status: *mut OmUint32,
            input_name_buffer: *const GssBuffer,
            input_name_type: *mut GssOidDesc,
            output_name: *mut GssName,
        ) -> OmUint32;
        fn gss_release_name(minor_status: *mut OmUint32, input_name: *mut GssName) -> OmUint32;
        fn gss_display_name(
            minor_status: *mut OmUint32,
            input_name: GssName,
            output_name_buffer: *mut GssBuffer,
            output_name_type: *mut *mut GssOidDesc,
        ) -> OmUint32;
        fn gss_acquire_cred(
            minor_status: *mut OmUint32,
            desired_name: GssName,
            time_req: OmUint32,
            desired_mechs: GssOidSet,
            cred_usage: c_int,
            output_cred_handle: *mut GssCred,
            actual_mechs: *mut GssOidSet,
            time_rec: *mut OmUint32,
        ) -> OmUint32;
        fn gss_release_cred(minor_status: *mut OmUint32, cred_handle: *mut GssCred) -> OmUint32;
        fn gss_init_sec_context(
            minor_status: *mut OmUint32,
            claimant_cred_handle: GssCred,
            context_handle: *mut GssCtx,
            target_name: GssName,
            mech_type: *mut GssOidDesc,
            req_flags: OmUint32,
            time_req: OmUint32,
            input_chan_bindings: GssChannelBindings,
            input_token: *const GssBuffer,
            actual_mech_type: *mut *mut GssOidDesc,
            output_token: *mut GssBuffer,
            ret_flags: *mut OmUint32,
            time_rec: *mut OmUint32,
        ) -> OmUint32;
        fn gss_accept_sec_context(
            minor_status: *mut OmUint32,
            context_handle: *mut GssCtx,
            acceptor_cred_handle: GssCred,
            input_token_buffer: *const GssBuffer,
            input_chan_bindings: GssChannelBindings,
            src_name: *mut GssName,
            mech_type: *mut *mut GssOidDesc,
            output_token: *mut GssBuffer,
            ret_flags: *mut OmUint32,
            time_rec: *mut OmUint32,
            delegated_cred_handle: *mut GssCred,
        ) -> OmUint32;
        fn gss_delete_sec_context(
            minor_status: *mut OmUint32,
            context_handle: *mut GssCtx,
            output_token: *mut GssBuffer,
        ) -> OmUint32;
        fn gss_release_buffer(minor_status: *mut OmUint32, buffer: *mut GssBuffer) -> OmUint32;
        fn gss_wrap(
            minor_status: *mut OmUint32,
            context_handle: GssCtx,
            conf_req_flag: c_int,
            qop_req: OmUint32,
            input_message_buffer: *const GssBuffer,
            conf_state: *mut c_int,
            output_message_buffer: *mut GssBuffer,
        ) -> OmUint32;
        fn gss_unwrap(
            minor_status: *mut OmUint32,
            context_handle: GssCtx,
            input_message_buffer: *const GssBuffer,
            output_message_buffer: *mut GssBuffer,
            conf_state: *mut c_int,
            qop_state: *mut OmUint32,
        ) -> OmUint32;
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum NameType {
        HostBased,
        UserName,
        Krb5Principal,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Error {
        pub major: OmUint32,
        pub minor: OmUint32,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Step {
        Continue(Vec<u8>),
        Complete(Vec<u8>),
    }

    pub struct ClientContext {
        context: SecurityContext,
        target_name: Name,
        credential: Option<Credential>,
    }

    pub struct ServerContext {
        context: SecurityContext,
        credential: Option<Credential>,
        source_name: Option<Name>,
    }

    struct SecurityContext {
        raw: GssCtx,
    }

    struct Name {
        raw: GssName,
    }

    struct Credential {
        raw: GssCred,
    }

    // SAFETY: GSS contexts are opaque handles managed by the platform GSS implementation. Socket
    // state protects them with a mutex and never aliases a context mutably across threads.
    unsafe impl Send for ClientContext {}
    // SAFETY: GSS contexts are opaque handles managed by the platform GSS implementation. Socket
    // state protects them with a mutex and never aliases a context mutably across threads.
    unsafe impl Send for ServerContext {}

    impl std::fmt::Debug for ClientContext {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("ClientContext")
                .finish_non_exhaustive()
        }
    }

    impl std::fmt::Debug for ServerContext {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("ServerContext")
                .finish_non_exhaustive()
        }
    }

    impl ClientContext {
        pub fn new(
            service: &[u8],
            service_name_type: NameType,
            principal: Option<(&[u8], NameType)>,
        ) -> Result<Self, Error> {
            Ok(Self {
                context: SecurityContext::new(),
                target_name: Name::import(service, service_name_type)?,
                credential: principal
                    .map(|(name, name_type)| Credential::acquire(name, name_type))
                    .transpose()?,
            })
        }

        pub fn initiate(&mut self, token: Option<&[u8]>) -> Result<Step, Error> {
            let input = token.map(BufferRef::new);
            let input_ptr = input.as_ref().map_or(ptr::null(), |buffer| buffer.as_ptr());
            let mut minor = 0;
            let mut output = GssBuffer::empty();
            let mut ret_flags = 0;
            // SAFETY: All GSS handles are either null sentinel values or live handles owned by
            // this wrapper. Input/output buffers point to valid memory for the call duration.
            let major = unsafe {
                gss_init_sec_context(
                    &mut minor,
                    self.credential
                        .as_ref()
                        .map_or(ptr::null_mut(), |cred| cred.raw),
                    &mut self.context.raw,
                    self.target_name.raw,
                    ptr::null_mut(),
                    GSS_C_MUTUAL_FLAG | GSS_C_REPLAY_FLAG,
                    0,
                    ptr::null_mut(),
                    input_ptr,
                    ptr::null_mut(),
                    &mut output,
                    &mut ret_flags,
                    ptr::null_mut(),
                )
            };
            step_from_status(major, minor, &mut output)
        }

        pub fn wrap(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
            self.context.wrap(plaintext)
        }

        pub fn unwrap(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, Error> {
            self.context.unwrap(ciphertext)
        }
    }

    impl ServerContext {
        pub fn new(principal: Option<(&[u8], NameType)>) -> Result<Self, Error> {
            Ok(Self {
                context: SecurityContext::new(),
                credential: principal
                    .map(|(name, name_type)| Credential::acquire(name, name_type))
                    .transpose()?,
                source_name: None,
            })
        }

        pub fn accept(&mut self, token: &[u8]) -> Result<Step, Error> {
            let input = BufferRef::new(token);
            let mut minor = 0;
            let mut output = GssBuffer::empty();
            let mut source_name = ptr::null_mut();
            let mut ret_flags = 0;
            // SAFETY: The input token references `token` for the duration of the call, output
            // pointers are valid, and owned credentials/context remain live while GSS uses them.
            let major = unsafe {
                gss_accept_sec_context(
                    &mut minor,
                    &mut self.context.raw,
                    self.credential
                        .as_ref()
                        .map_or(ptr::null_mut(), |cred| cred.raw),
                    input.as_ptr(),
                    ptr::null_mut(),
                    &mut source_name,
                    ptr::null_mut(),
                    &mut output,
                    &mut ret_flags,
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            };
            if !source_name.is_null() {
                self.source_name = Some(Name { raw: source_name });
            }
            step_from_status(major, minor, &mut output)
        }

        pub fn source_principal(&self) -> Result<Vec<u8>, Error> {
            self.source_name
                .as_ref()
                .ok_or(Error { major: 1, minor: 0 })?
                .display()
        }

        pub fn wrap(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
            self.context.wrap(plaintext)
        }

        pub fn unwrap(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, Error> {
            self.context.unwrap(ciphertext)
        }
    }

    impl SecurityContext {
        fn new() -> Self {
            Self {
                raw: ptr::null_mut(),
            }
        }

        fn wrap(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
            let input = BufferRef::new(plaintext);
            let mut minor = 0;
            let mut output = GssBuffer::empty();
            let mut confidential = 0;
            // SAFETY: `self.raw` is an established context and buffers are valid for the call.
            let major = unsafe {
                gss_wrap(
                    &mut minor,
                    self.raw,
                    1,
                    GSS_C_QOP_DEFAULT,
                    input.as_ptr(),
                    &mut confidential,
                    &mut output,
                )
            };
            if major != GSS_S_COMPLETE || confidential == 0 {
                release_buffer(&mut output);
                return Err(Error { major, minor });
            }
            Ok(copy_and_release(&mut output))
        }

        fn unwrap(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, Error> {
            let input = BufferRef::new(ciphertext);
            let mut minor = 0;
            let mut output = GssBuffer::empty();
            let mut confidential = 0;
            // SAFETY: `self.raw` is an established context and buffers are valid for the call.
            let major = unsafe {
                gss_unwrap(
                    &mut minor,
                    self.raw,
                    input.as_ptr(),
                    &mut output,
                    &mut confidential,
                    ptr::null_mut(),
                )
            };
            if major != GSS_S_COMPLETE || confidential == 0 {
                release_buffer(&mut output);
                return Err(Error { major, minor });
            }
            Ok(copy_and_release(&mut output))
        }
    }

    impl Drop for SecurityContext {
        fn drop(&mut self) {
            if !self.raw.is_null() {
                let mut minor = 0;
                // SAFETY: `self.raw` is owned by this wrapper and may be deleted once here.
                unsafe {
                    gss_delete_sec_context(&mut minor, &mut self.raw, ptr::null_mut());
                }
            }
        }
    }

    impl Name {
        fn import(name: &[u8], name_type: NameType) -> Result<Self, Error> {
            let mut owned = Vec::with_capacity(name.len() + 1);
            owned.extend_from_slice(name);
            owned.push(0);
            let input = BufferRef::new(&owned);
            let mut oid = oid_for(name_type);
            let mut minor = 0;
            let mut raw = ptr::null_mut();
            // SAFETY: The name buffer and OID descriptor are valid for the call, and `raw` is a
            // valid output slot for the imported GSS name.
            let major = unsafe { gss_import_name(&mut minor, input.as_ptr(), &mut oid, &mut raw) };
            if major != GSS_S_COMPLETE {
                return Err(Error { major, minor });
            }
            Ok(Self { raw })
        }

        fn display(&self) -> Result<Vec<u8>, Error> {
            let mut minor = 0;
            let mut output = GssBuffer::empty();
            // SAFETY: `self.raw` is a live GSS name and `output` is a valid output buffer.
            let major =
                unsafe { gss_display_name(&mut minor, self.raw, &mut output, ptr::null_mut()) };
            if major != GSS_S_COMPLETE {
                release_buffer(&mut output);
                return Err(Error { major, minor });
            }
            Ok(copy_and_release(&mut output))
        }
    }

    impl Drop for Name {
        fn drop(&mut self) {
            if !self.raw.is_null() {
                let mut minor = 0;
                // SAFETY: `self.raw` is owned by this wrapper and released exactly once here.
                unsafe {
                    gss_release_name(&mut minor, &mut self.raw);
                }
            }
        }
    }

    impl Credential {
        fn acquire(name: &[u8], name_type: NameType) -> Result<Self, Error> {
            let imported = Name::import(name, name_type)?;
            let mut minor = 0;
            let mut raw = ptr::null_mut();
            // SAFETY: `imported.raw` is a live GSS name and `raw` is a valid output slot.
            let major = unsafe {
                gss_acquire_cred(
                    &mut minor,
                    imported.raw,
                    0,
                    ptr::null_mut(),
                    GSS_C_BOTH,
                    &mut raw,
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            };
            if major != GSS_S_COMPLETE {
                return Err(Error { major, minor });
            }
            Ok(Self { raw })
        }
    }

    impl Drop for Credential {
        fn drop(&mut self) {
            if !self.raw.is_null() {
                let mut minor = 0;
                // SAFETY: `self.raw` is owned by this wrapper and released exactly once here.
                unsafe {
                    gss_release_cred(&mut minor, &mut self.raw);
                }
            }
        }
    }

    struct BufferRef {
        buffer: GssBuffer,
    }

    impl BufferRef {
        fn new(bytes: &[u8]) -> Self {
            Self {
                buffer: GssBuffer {
                    length: bytes.len(),
                    value: bytes.as_ptr().cast::<c_void>().cast_mut(),
                },
            }
        }

        fn as_ptr(&self) -> *const GssBuffer {
            &self.buffer
        }
    }

    impl GssBuffer {
        fn empty() -> Self {
            Self {
                length: 0,
                value: ptr::null_mut(),
            }
        }
    }

    fn step_from_status(
        major: OmUint32,
        minor: OmUint32,
        output: &mut GssBuffer,
    ) -> Result<Step, Error> {
        let token = copy_and_release(output);
        match major {
            GSS_S_COMPLETE => Ok(Step::Complete(token)),
            GSS_S_CONTINUE_NEEDED => Ok(Step::Continue(token)),
            _ => Err(Error { major, minor }),
        }
    }

    fn copy_and_release(buffer: &mut GssBuffer) -> Vec<u8> {
        let bytes = if buffer.value.is_null() || buffer.length == 0 {
            Vec::new()
        } else {
            // SAFETY: GSS returned `value` with `length` bytes and keeps it valid until released.
            unsafe { std::slice::from_raw_parts(buffer.value.cast::<u8>(), buffer.length).to_vec() }
        };
        release_buffer(buffer);
        bytes
    }

    fn release_buffer(buffer: &mut GssBuffer) {
        if !buffer.value.is_null() {
            let mut minor = 0;
            // SAFETY: GSS allocated this buffer and expects it to be released by this function.
            unsafe {
                gss_release_buffer(&mut minor, buffer);
            }
            buffer.value = ptr::null_mut();
            buffer.length = 0;
        }
    }

    fn oid_for(name_type: NameType) -> GssOidDesc {
        let bytes = match name_type {
            NameType::HostBased => OID_HOSTBASED_SERVICE,
            NameType::UserName => OID_USER_NAME,
            NameType::Krb5Principal => OID_KRB5_PRINCIPAL,
        };
        GssOidDesc {
            length: bytes.len() as OmUint32,
            elements: bytes.as_ptr().cast::<c_void>().cast_mut(),
        }
    }
}

#[cfg(feature = "sodium")]
pub mod sodium {
    use std::ffi::{c_int, c_uchar, c_ulonglong};
    use std::sync::OnceLock;

    const KEY_SIZE: usize = 32;
    const NONCE_SIZE: usize = 24;
    const MAC_SIZE: usize = 16;

    #[link(name = "sodium")]
    unsafe extern "C" {
        fn sodium_init() -> c_int;
        fn crypto_box_beforenm(
            key: *mut c_uchar,
            public_key: *const c_uchar,
            secret_key: *const c_uchar,
        ) -> c_int;
        fn crypto_box_easy_afternm(
            ciphertext: *mut c_uchar,
            message: *const c_uchar,
            message_len: c_ulonglong,
            nonce: *const c_uchar,
            key: *const c_uchar,
        ) -> c_int;
        fn crypto_box_open_easy_afternm(
            message: *mut c_uchar,
            ciphertext: *const c_uchar,
            ciphertext_len: c_ulonglong,
            nonce: *const c_uchar,
            key: *const c_uchar,
        ) -> c_int;
    }

    pub fn crypto_box_beforenm_key(
        public_key: &[u8; KEY_SIZE],
        secret_key: &[u8; KEY_SIZE],
    ) -> Option<[u8; KEY_SIZE]> {
        ensure_initialized()?;
        let mut key = [0; KEY_SIZE];
        // SAFETY: All pointers reference fixed-size initialized byte arrays with libsodium's
        // documented key lengths, and `key` is writable for `KEY_SIZE` bytes.
        let rc = unsafe {
            crypto_box_beforenm(key.as_mut_ptr(), public_key.as_ptr(), secret_key.as_ptr())
        };
        (rc == 0).then_some(key)
    }

    pub fn crypto_box_easy_afternm_encrypt(
        message: &[u8],
        nonce: &[u8; NONCE_SIZE],
        key: &[u8; KEY_SIZE],
    ) -> Option<Vec<u8>> {
        ensure_initialized()?;
        let mut ciphertext = vec![0; message.len() + MAC_SIZE];
        // SAFETY: `ciphertext` is writable for `message.len() + MAC_SIZE` bytes; message,
        // nonce, and key pointers are valid for libsodium's documented input sizes.
        let rc = unsafe {
            crypto_box_easy_afternm(
                ciphertext.as_mut_ptr(),
                message.as_ptr(),
                message.len() as c_ulonglong,
                nonce.as_ptr(),
                key.as_ptr(),
            )
        };
        (rc == 0).then_some(ciphertext)
    }

    pub fn crypto_box_easy_afternm_encrypt_into(
        ciphertext: &mut [u8],
        message: &[u8],
        nonce: &[u8; NONCE_SIZE],
        key: &[u8; KEY_SIZE],
    ) -> Option<()> {
        if ciphertext.len() != message.len() + MAC_SIZE {
            return None;
        }
        ensure_initialized()?;
        // SAFETY: `ciphertext` is writable for exactly `message.len() + MAC_SIZE` bytes;
        // message, nonce, and key pointers are valid for libsodium's documented input sizes.
        let rc = unsafe {
            crypto_box_easy_afternm(
                ciphertext.as_mut_ptr(),
                message.as_ptr(),
                message.len() as c_ulonglong,
                nonce.as_ptr(),
                key.as_ptr(),
            )
        };
        (rc == 0).then_some(())
    }

    pub fn crypto_box_easy_afternm_decrypt(
        ciphertext: &[u8],
        nonce: &[u8; NONCE_SIZE],
        key: &[u8; KEY_SIZE],
    ) -> Option<Vec<u8>> {
        if ciphertext.len() < MAC_SIZE {
            return None;
        }
        ensure_initialized()?;
        let mut message = vec![0; ciphertext.len() - MAC_SIZE];
        // SAFETY: `message` is writable for `ciphertext.len() - MAC_SIZE` bytes; ciphertext,
        // nonce, and key pointers are valid for libsodium's documented input sizes.
        let rc = unsafe {
            crypto_box_open_easy_afternm(
                message.as_mut_ptr(),
                ciphertext.as_ptr(),
                ciphertext.len() as c_ulonglong,
                nonce.as_ptr(),
                key.as_ptr(),
            )
        };
        (rc == 0).then_some(message)
    }

    fn ensure_initialized() -> Option<()> {
        static INIT: OnceLock<bool> = OnceLock::new();
        (*INIT.get_or_init(|| {
            // SAFETY: `sodium_init` is process-global initialization designed to be called
            // multiple times; libsodium handles concurrent/repeated calls internally.
            unsafe { sodium_init() >= 0 }
        }))
        .then_some(())
    }
}

use std::io;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::time::Duration;

pub const POLLIN: i16 = 0x001;
pub const POLLOUT: i16 = 0x004;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressFamily {
    Inet,
    Inet6,
    Unix,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SockAddr {
    Inet(SocketAddr),
    UnixPath(Vec<u8>),
}

#[derive(Debug)]
pub struct TcpListenerHandle {
    listener: TcpListener,
}

#[derive(Debug)]
pub struct TcpStreamHandle {
    stream: TcpStream,
}

#[derive(Debug)]
pub struct UdpSocketHandle {
    socket: UdpSocket,
}

impl SockAddr {
    pub fn family(&self) -> AddressFamily {
        match self {
            Self::Inet(addr) if matches!(addr.ip(), IpAddr::V6(_)) => AddressFamily::Inet6,
            Self::Inet(_) => AddressFamily::Inet,
            Self::UnixPath(_) => AddressFamily::Unix,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollEvent {
    pub fd: RawHandle,
    pub events: i16,
    pub revents: i16,
}

impl PollEvent {
    pub fn new(fd: RawHandle, events: i16) -> Self {
        Self {
            fd,
            events,
            revents: 0,
        }
    }
}

impl TcpListenerHandle {
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(addr)?,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.listener.set_nonblocking(nonblocking)
    }

    pub fn accept(&self) -> io::Result<TcpStreamHandle> {
        let (stream, _) = self.listener.accept()?;
        Ok(TcpStreamHandle { stream })
    }
}

impl TcpStreamHandle {
    pub fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        Ok(Self {
            stream: TcpStream::connect(addr)?,
        })
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.stream.set_nonblocking(nonblocking)
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(timeout)
    }

    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_write_timeout(timeout)
    }

    pub fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.stream.write_all(bytes)
    }

    pub fn read_exact(&mut self, bytes: &mut [u8]) -> io::Result<()> {
        self.stream.read_exact(bytes)
    }

    pub fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.stream.read(bytes)
    }
}

impl Read for TcpStreamHandle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf)
    }
}

impl Write for TcpStreamHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

impl UdpSocketHandle {
    pub fn bind(addr: impl ToSocketAddrs) -> io::Result<Self> {
        Ok(Self {
            socket: UdpSocket::bind(addr)?,
        })
    }

    pub fn connect(&self, addr: impl ToSocketAddrs) -> io::Result<()> {
        self.socket.connect(addr)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.socket.set_nonblocking(nonblocking)
    }

    pub fn join_multicast_v4(&self, group: Ipv4Addr, interface: Ipv4Addr) -> io::Result<()> {
        self.socket.join_multicast_v4(&group, &interface)
    }

    pub fn set_multicast_loop_v4(&self, enabled: bool) -> io::Result<()> {
        self.socket.set_multicast_loop_v4(enabled)
    }

    pub fn set_multicast_ttl_v4(&self, ttl: u32) -> io::Result<()> {
        self.socket.set_multicast_ttl_v4(ttl)
    }

    #[cfg(unix)]
    pub fn set_multicast_if_v4(&self, interface: Ipv4Addr) -> io::Result<()> {
        let addr = libc::in_addr {
            s_addr: u32::from_ne_bytes(interface.octets()),
        };
        // SAFETY: `self.socket.as_raw_fd()` is a valid UDP socket fd, and `addr` points to a
        // properly initialized `in_addr` for the duration of the call.
        let rc = unsafe {
            libc::setsockopt(
                self.socket.as_raw_fd(),
                libc::IPPROTO_IP,
                libc::IP_MULTICAST_IF,
                (&addr as *const libc::in_addr).cast(),
                std::mem::size_of::<libc::in_addr>() as libc::socklen_t,
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(windows)]
    pub fn set_multicast_if_v4(&self, _interface: Ipv4Addr) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "setting IPv4 multicast interface is not implemented on Windows",
        ))
    }

    pub fn send(&self, bytes: &[u8]) -> io::Result<usize> {
        self.socket.send(bytes)
    }

    pub fn send_to(&self, bytes: &[u8], addr: SocketAddr) -> io::Result<usize> {
        self.socket.send_to(bytes, addr)
    }

    pub fn recv_from(&self, bytes: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.socket.recv_from(bytes)
    }
}

#[cfg(unix)]
pub mod ipc {
    use std::io::{self, Read, Write};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::Path;

    #[derive(Debug)]
    pub struct IpcListenerHandle {
        listener: UnixListener,
    }

    #[derive(Debug)]
    pub struct IpcStreamHandle {
        stream: UnixStream,
    }

    impl IpcListenerHandle {
        pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
            let path = path.as_ref();
            if path.exists() {
                std::fs::remove_file(path)?;
            }
            Ok(Self {
                listener: UnixListener::bind(path)?,
            })
        }

        pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
            self.listener.set_nonblocking(nonblocking)
        }

        pub fn accept(&self) -> io::Result<IpcStreamHandle> {
            let (stream, _) = self.listener.accept()?;
            Ok(IpcStreamHandle { stream })
        }
    }

    impl IpcStreamHandle {
        pub fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
            Ok(Self {
                stream: UnixStream::connect(path)?,
            })
        }

        pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
            self.stream.set_nonblocking(nonblocking)
        }

        pub fn set_read_timeout(&self, timeout: Option<std::time::Duration>) -> io::Result<()> {
            self.stream.set_read_timeout(timeout)
        }

        pub fn set_write_timeout(&self, timeout: Option<std::time::Duration>) -> io::Result<()> {
            self.stream.set_write_timeout(timeout)
        }

        pub fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
            self.stream.write_all(bytes)
        }

        pub fn read_exact(&mut self, bytes: &mut [u8]) -> io::Result<()> {
            self.stream.read_exact(bytes)
        }

        pub fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            self.stream.read(bytes)
        }
    }
}

#[cfg(windows)]
pub mod ipc {
    use std::io;

    #[derive(Debug)]
    pub struct IpcListenerHandle;

    #[derive(Debug)]
    pub struct IpcStreamHandle;

    impl IpcListenerHandle {
        pub fn bind(_path: impl AsRef<std::path::Path>) -> io::Result<Self> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC listener is unavailable on Windows in this layer",
            ))
        }

        pub fn set_nonblocking(&self, _nonblocking: bool) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC listener is unavailable on Windows",
            ))
        }

        pub fn accept(&self) -> io::Result<IpcStreamHandle> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC listener is unavailable on Windows",
            ))
        }
    }

    impl IpcStreamHandle {
        pub fn connect(_path: impl AsRef<std::path::Path>) -> io::Result<Self> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC connecter is unavailable on Windows in this layer",
            ))
        }

        pub fn set_nonblocking(&self, _nonblocking: bool) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC stream is unavailable on Windows",
            ))
        }

        pub fn set_read_timeout(&self, _timeout: Option<std::time::Duration>) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC stream is unavailable on Windows",
            ))
        }

        pub fn set_write_timeout(&self, _timeout: Option<std::time::Duration>) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC stream is unavailable on Windows",
            ))
        }

        pub fn write_all(&mut self, _bytes: &[u8]) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC stream is unavailable on Windows",
            ))
        }

        pub fn read_exact(&mut self, _bytes: &mut [u8]) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC stream is unavailable on Windows",
            ))
        }

        pub fn read(&mut self, _bytes: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "IPC stream is unavailable on Windows",
            ))
        }
    }
}

#[cfg(unix)]
mod imp {
    use super::{io, Duration, PollEvent};
    use std::mem::MaybeUninit;
    use std::os::fd::RawFd;

    pub type RawHandle = RawFd;

    #[derive(Debug)]
    pub struct OwnedHandle {
        fd: RawFd,
    }

    impl OwnedHandle {
        pub fn new(fd: RawFd) -> io::Result<Self> {
            if fd < 0 {
                Err(io::Error::from_raw_os_error(libc::EBADF))
            } else {
                Ok(Self { fd })
            }
        }

        pub fn raw(&self) -> RawFd {
            self.fd
        }

        pub fn into_raw(mut self) -> RawFd {
            let fd = self.fd;
            self.fd = -1;
            fd
        }

        pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
            set_nonblocking(self.fd, nonblocking)
        }
    }

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            if self.fd >= 0 {
                // SAFETY: `fd` is owned by this RAII wrapper and is closed at most once.
                unsafe { libc::close(self.fd) };
            }
        }
    }

    #[derive(Debug)]
    pub struct SignalPair {
        reader: OwnedHandle,
        writer: OwnedHandle,
    }

    impl SignalPair {
        pub fn new() -> io::Result<Self> {
            let mut fds = [0; 2];
            // SAFETY: `fds` points to two valid integers for libc to initialize.
            let rc =
                unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
            if rc != 0 {
                return Err(io::Error::last_os_error());
            }
            let pair = Self {
                reader: OwnedHandle::new(fds[0])?,
                writer: OwnedHandle::new(fds[1])?,
            };
            pair.reader.set_nonblocking(true)?;
            pair.writer.set_nonblocking(true)?;
            Ok(pair)
        }

        pub fn reader(&self) -> RawFd {
            self.reader.raw()
        }

        pub fn writer(&self) -> RawFd {
            self.writer.raw()
        }

        pub fn signal(&self) -> io::Result<()> {
            let byte = [1u8];
            // SAFETY: `writer` is a valid owned socket fd and the byte slice is valid for length 1.
            let rc = unsafe { libc::write(self.writer.raw(), byte.as_ptr().cast(), byte.len()) };
            if rc < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }

        pub fn drain(&self) -> io::Result<usize> {
            let mut total = 0;
            let mut buf = [0u8; 64];
            loop {
                // SAFETY: `reader` is a valid owned socket fd and `buf` is writable for `buf.len()` bytes.
                let rc =
                    unsafe { libc::read(self.reader.raw(), buf.as_mut_ptr().cast(), buf.len()) };
                if rc > 0 {
                    total += rc as usize;
                    continue;
                }
                if rc == 0 {
                    return Ok(total);
                }
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    return Ok(total);
                }
                return Err(err);
            }
        }
    }

    pub fn set_nonblocking(fd: RawFd, nonblocking: bool) -> io::Result<()> {
        // SAFETY: `fd` is supplied by caller and `F_GETFL` does not require an additional argument.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        let updated = if nonblocking {
            flags | libc::O_NONBLOCK
        } else {
            flags & !libc::O_NONBLOCK
        };
        // SAFETY: `fd` is supplied by caller and `updated` is a valid file status flag set.
        let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, updated) };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn pipe() -> io::Result<(OwnedHandle, OwnedHandle)> {
        let mut fds = [0; 2];
        // SAFETY: `fds` points to two valid integers for libc to initialize.
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok((OwnedHandle::new(fds[0])?, OwnedHandle::new(fds[1])?))
    }

    pub fn poll(events: &mut [PollEvent], timeout: Option<Duration>) -> io::Result<usize> {
        let timeout_ms = timeout.map(duration_to_ms).unwrap_or(-1);
        let mut pollfds: Vec<libc::pollfd> = events
            .iter()
            .map(|event| libc::pollfd {
                fd: event.fd,
                events: event.events,
                revents: 0,
            })
            .collect();
        // SAFETY: `pollfds` points to `pollfds.len()` initialized pollfd entries.
        let rc = unsafe {
            libc::poll(
                pollfds.as_mut_ptr(),
                pollfds.len() as libc::nfds_t,
                timeout_ms,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        for (event, pollfd) in events.iter_mut().zip(pollfds.iter()) {
            event.revents = pollfd.revents;
        }
        Ok(rc as usize)
    }

    pub fn select_read(fd: RawFd, timeout: Option<Duration>) -> io::Result<bool> {
        let mut readfds = MaybeUninit::<libc::fd_set>::uninit();
        // SAFETY: `readfds` is allocated and then initialized through FD_ZERO/FD_SET before use.
        unsafe {
            libc::FD_ZERO(readfds.as_mut_ptr());
            libc::FD_SET(fd, readfds.as_mut_ptr());
        }
        let mut timeout_value = timeout.map(duration_to_timeval);
        let timeout_ptr = timeout_value
            .as_mut()
            .map_or(std::ptr::null_mut(), |value| value as *mut libc::timeval);
        // SAFETY: `readfds` is initialized, other fd sets are null, and `timeout_ptr` is null or valid.
        let rc = unsafe {
            libc::select(
                fd + 1,
                readfds.as_mut_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                timeout_ptr,
            )
        };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(rc > 0)
        }
    }

    pub fn create_tcp_socket() -> io::Result<OwnedHandle> {
        // SAFETY: Arguments are valid constants for creating a TCP socket.
        let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
        OwnedHandle::new(fd)
    }

    pub fn create_ipc_socket() -> io::Result<OwnedHandle> {
        // SAFETY: Arguments are valid constants for creating a Unix domain stream socket.
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        OwnedHandle::new(fd)
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn create_epoll() -> io::Result<OwnedHandle> {
        // SAFETY: `EPOLL_CLOEXEC` is a valid epoll_create1 flag.
        let fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        OwnedHandle::new(fd)
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub fn create_epoll() -> io::Result<OwnedHandle> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "epoll is unavailable on this platform",
        ))
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly"
    ))]
    pub fn create_kqueue() -> io::Result<OwnedHandle> {
        // SAFETY: `kqueue` takes no arguments and returns a new kernel queue fd on success.
        let fd = unsafe { libc::kqueue() };
        OwnedHandle::new(fd)
    }

    #[cfg(not(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly"
    )))]
    pub fn create_kqueue() -> io::Result<OwnedHandle> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "kqueue is unavailable on this platform",
        ))
    }

    fn duration_to_ms(duration: Duration) -> i32 {
        i32::try_from(duration.as_millis()).unwrap_or(i32::MAX)
    }

    fn duration_to_timeval(duration: Duration) -> libc::timeval {
        libc::timeval {
            tv_sec: duration.as_secs() as libc::time_t,
            tv_usec: duration.subsec_micros() as libc::suseconds_t,
        }
    }
}

#[cfg(windows)]
mod imp {
    use super::{io, Duration, PollEvent, POLLIN};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::windows::io::{AsRawSocket, IntoRawSocket, RawSocket};
    use std::time::Instant;
    use windows_sys::Win32::Networking::WinSock::{
        closesocket, ioctlsocket, FIONBIO, POLLRDNORM, POLLWRNORM, SOCKET_ERROR,
    };

    pub type RawHandle = RawSocket;
    const INVALID_SOCKET_VALUE: RawSocket = !0;

    #[derive(Debug)]
    pub struct OwnedHandle {
        socket: RawSocket,
    }

    impl OwnedHandle {
        pub fn new(socket: RawSocket) -> io::Result<Self> {
            if socket == INVALID_SOCKET_VALUE {
                Err(io::Error::last_os_error())
            } else {
                Ok(Self { socket })
            }
        }

        pub fn raw(&self) -> RawSocket {
            self.socket
        }

        pub fn into_raw(mut self) -> RawSocket {
            let socket = self.socket;
            self.socket = INVALID_SOCKET_VALUE;
            socket
        }

        pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
            set_nonblocking(self.socket, nonblocking)
        }
    }

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            if self.socket != INVALID_SOCKET_VALUE {
                // SAFETY: `socket` is owned by this RAII wrapper and is closed at most once.
                unsafe { closesocket(self.socket as usize) };
            }
        }
    }

    #[derive(Debug)]
    pub struct SignalPair {
        reader: TcpStream,
        writer: TcpStream,
    }

    impl SignalPair {
        pub fn new() -> io::Result<Self> {
            let listener = TcpListener::bind(("127.0.0.1", 0))?;
            let writer = TcpStream::connect(listener.local_addr()?)?;
            let (reader, _) = listener.accept()?;
            reader.set_nonblocking(true)?;
            writer.set_nonblocking(true)?;
            Ok(Self { reader, writer })
        }

        pub fn reader(&self) -> RawSocket {
            self.reader.as_raw_socket()
        }

        pub fn writer(&self) -> RawSocket {
            self.writer.as_raw_socket()
        }

        pub fn signal(&self) -> io::Result<()> {
            let mut writer = &self.writer;
            writer.write_all(&[1])
        }

        pub fn drain(&self) -> io::Result<usize> {
            let mut total = 0;
            let mut buf = [0u8; 64];
            loop {
                let mut reader = &self.reader;
                match reader.read(&mut buf) {
                    Ok(0) => return Ok(total),
                    Ok(n) => total += n,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(total),
                    Err(error) => return Err(error),
                }
            }
        }
    }

    pub fn set_nonblocking(socket: RawSocket, nonblocking: bool) -> io::Result<()> {
        let mut mode = u32::from(nonblocking);
        // SAFETY: `socket` is supplied by caller and `mode` points to a valid u_long value.
        let rc = unsafe { ioctlsocket(socket as usize, FIONBIO, &mut mode) };
        if rc == SOCKET_ERROR {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub fn pipe() -> io::Result<(OwnedHandle, OwnedHandle)> {
        let pair = SignalPair::new()?;
        Ok((
            OwnedHandle::new(pair.reader.into_raw_socket())?,
            OwnedHandle::new(pair.writer.into_raw_socket())?,
        ))
    }

    pub fn poll(events: &mut [PollEvent], timeout: Option<Duration>) -> io::Result<usize> {
        let timeout_at = timeout.map(|timeout| Instant::now() + timeout);
        let mut ready = 0;
        for event in events.iter_mut() {
            event.revents = 0;
            if event.events & POLLIN != 0 {
                event.revents |= POLLRDNORM as i16;
            }
            if event.events & super::POLLOUT != 0 {
                event.revents |= POLLWRNORM as i16;
            }
            if event.revents != 0 {
                ready += 1;
            }
        }
        if ready == 0 {
            if let Some(timeout_at) = timeout_at {
                let now = Instant::now();
                if timeout_at > now {
                    std::thread::sleep(timeout_at - now);
                }
            }
        }
        Ok(ready)
    }

    pub fn select_read(_fd: RawSocket, timeout: Option<Duration>) -> io::Result<bool> {
        if let Some(timeout) = timeout {
            std::thread::sleep(timeout);
        }
        Ok(false)
    }

    pub fn create_tcp_socket() -> io::Result<OwnedHandle> {
        let stream = TcpStream::connect(("127.0.0.1", 9))?;
        OwnedHandle::new(stream.into_raw_socket())
    }

    pub fn create_ipc_socket() -> io::Result<OwnedHandle> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "IPC sockets are unavailable on Windows in this layer",
        ))
    }

    pub fn create_epoll() -> io::Result<OwnedHandle> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "epoll is unavailable on Windows",
        ))
    }

    pub fn create_kqueue() -> io::Result<OwnedHandle> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "kqueue is unavailable on Windows",
        ))
    }
}

pub use imp::{
    create_epoll, create_ipc_socket, create_kqueue, create_tcp_socket, pipe, poll, select_read,
    set_nonblocking,
};
pub use imp::{OwnedHandle, RawHandle, SignalPair};

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::thread;

    #[test]
    fn sockaddr_reports_family() {
        let inet = SockAddr::Inet(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 5555).into());
        assert_eq!(inet.family(), AddressFamily::Inet);
        let unix = SockAddr::UnixPath(b"/tmp/libzmq-test.sock".to_vec());
        assert_eq!(unix.family(), AddressFamily::Unix);
    }

    #[test]
    fn invalid_handle_is_rejected() {
        #[cfg(unix)]
        assert!(OwnedHandle::new(-1).is_err());

        #[cfg(windows)]
        assert!(OwnedHandle::new(!0).is_err());
    }

    #[test]
    fn pipe_handles_support_nonblocking() {
        let (reader, writer) = pipe().expect("pipe should be created");
        reader
            .set_nonblocking(true)
            .expect("reader should become nonblocking");
        writer
            .set_nonblocking(true)
            .expect("writer should become nonblocking");
    }

    #[test]
    fn signaler_wakes_poll_and_select() {
        let signaler = SignalPair::new().expect("signaler should be created");
        let mut events = [PollEvent::new(signaler.reader(), POLLIN)];
        assert_eq!(
            poll(&mut events, Some(Duration::from_millis(0))).unwrap(),
            0
        );
        signaler.signal().expect("signal should write a byte");
        assert_eq!(
            poll(&mut events, Some(Duration::from_millis(100))).unwrap(),
            1
        );
        assert!(events[0].revents & POLLIN != 0);
        assert!(select_read(signaler.reader(), Some(Duration::from_millis(100))).unwrap());
        assert!(signaler.drain().unwrap() > 0);
    }

    #[test]
    fn tcp_and_ipc_socket_syscalls_are_wrapped() {
        let tcp = create_tcp_socket().expect("TCP socket should be created");
        tcp.set_nonblocking(true)
            .expect("TCP socket should become nonblocking");

        #[cfg(unix)]
        {
            let ipc = create_ipc_socket().expect("IPC socket should be created");
            ipc.set_nonblocking(true)
                .expect("IPC socket should become nonblocking");
        }
    }

    #[test]
    fn tcp_listener_and_connecter_exchange_bytes() {
        let listener = TcpListenerHandle::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let mut stream = listener.accept().unwrap();
            let mut bytes = [0u8; 4];
            stream.read_exact(&mut bytes).unwrap();
            assert_eq!(&bytes, b"ping");
            stream.write_all(b"pong").unwrap();
        });

        let mut client = TcpStreamHandle::connect(addr).unwrap();
        client.write_all(b"ping").unwrap();
        let mut bytes = [0u8; 4];
        client.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"pong");
        server.join().unwrap();
    }

    #[test]
    fn udp_sockets_exchange_datagrams() {
        let server = UdpSocketHandle::bind("127.0.0.1:0").unwrap();
        let client = UdpSocketHandle::bind("127.0.0.1:0").unwrap();
        let server_addr = server.local_addr().unwrap();
        client.connect(server_addr).unwrap();

        assert_eq!(client.send(b"ping").unwrap(), 4);
        let mut bytes = [0u8; 8];
        let (size, peer) = server.recv_from(&mut bytes).unwrap();
        assert_eq!(&bytes[..size], b"ping");

        assert_eq!(server.send_to(b"pong", peer).unwrap(), 4);
        let (size, _) = client.recv_from(&mut bytes).unwrap();
        assert_eq!(&bytes[..size], b"pong");
    }

    #[cfg(unix)]
    #[test]
    fn ipc_listener_and_connecter_exchange_bytes() {
        let path = std::env::temp_dir().join(format!(
            "libzmq-ipc-{}-{}.sock",
            std::process::id(),
            "exchange"
        ));
        let listener = ipc::IpcListenerHandle::bind(&path).unwrap();
        let server = thread::spawn(move || {
            let mut stream = listener.accept().unwrap();
            let mut bytes = [0u8; 4];
            stream.read_exact(&mut bytes).unwrap();
            assert_eq!(&bytes, b"ping");
            stream.write_all(b"pong").unwrap();
        });

        let mut client = ipc::IpcStreamHandle::connect(&path).unwrap();
        client.write_all(b"ping").unwrap();
        let mut bytes = [0u8; 4];
        client.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"pong");
        server.join().unwrap();
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn native_poll_backends_are_constructible_or_explicitly_unsupported() {
        match create_epoll() {
            Ok(handle) => drop(handle),
            Err(error) => assert_eq!(error.kind(), io::ErrorKind::Unsupported),
        }
        match create_kqueue() {
            Ok(handle) => drop(handle),
            Err(error) => assert_eq!(error.kind(), io::ErrorKind::Unsupported),
        }
    }

    #[cfg(feature = "norm")]
    #[test]
    fn norm_library_version_is_available() {
        let version = crate::norm::version().expect("NORM library should report a version");
        assert!(version.major >= 1);
    }

    #[cfg(feature = "norm")]
    #[test]
    fn norm_data_object_round_trip_is_available() {
        let receiver_instance = crate::norm::Instance::new().expect("receiver instance");
        let sender_instance = crate::norm::Instance::new().expect("sender instance");
        let port = 20_000 + (std::process::id() % 20_000) as u16;
        let mut receiver_config = crate::norm::SessionConfig::new("127.0.0.1", port, 1);
        let mut sender_config = crate::norm::SessionConfig::new("127.0.0.1", port, 2);
        receiver_config.buffer_space = 512 * 1024;
        sender_config.buffer_space = 512 * 1024;

        let mut receiver = receiver_instance
            .create_session(&receiver_config)
            .expect("receiver session");
        receiver
            .start_receiver(receiver_config.buffer_space)
            .expect("receiver should start");
        let mut sender = sender_instance
            .create_session(&sender_config)
            .expect("sender session");
        sender
            .start_sender(&sender_config)
            .expect("sender should start");

        sender.send_data(b"norm-ping").expect("data enqueue");
        assert_eq!(
            receiver_instance
                .recv_data(Duration::from_secs(3))
                .as_deref(),
            Some(&b"norm-ping"[..])
        );
    }
}
