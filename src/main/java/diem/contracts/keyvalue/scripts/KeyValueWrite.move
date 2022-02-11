script {
    use 0x1::KeyValue;

    fun main(owner: address,  key: u64, value: u64) {
        KeyValue::set(owner, key, value);
    }
}
