script {
    use 0x1::KeyValue;

    fun main(account: signer, key: u64) {
        return KeyValue::set(&account, key)
    }
}
