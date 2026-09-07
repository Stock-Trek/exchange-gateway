pub trait ServerTimeResponse {
    fn server_time(&self) -> u64;
}
