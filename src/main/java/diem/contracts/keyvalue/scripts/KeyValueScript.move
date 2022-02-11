script {
    use 0x1::KeyValue;

    fun main(account: signer) {
        KeyValue::create_collection(account);
    }
}
