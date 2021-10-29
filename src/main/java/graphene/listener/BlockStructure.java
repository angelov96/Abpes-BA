package graphene.listener;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class BlockStructure {

    @SerializedName("id")
    @Expose
    private String id;
    @SerializedName("account")
    @Expose
    private String account;
    @SerializedName("operation_id")
    @Expose
    private String operationId;
    @SerializedName("sequence")
    @Expose
    private Integer sequence;
    @SerializedName("next")
    @Expose
    private String next;
    @SerializedName("op")
    @Expose
    private List<?> op = null;
    @SerializedName("result")
    @Expose
    private List<?> result = null;
    @SerializedName("block_num")
    @Expose
    private Integer blockNum;
    @SerializedName("trx_in_block")
    @Expose
    private Integer trxInBlock;
    @SerializedName("op_in_trx")
    @Expose
    private Integer opInTrx;
    @SerializedName("virtual_op")
    @Expose
    private Integer virtualOp;
    @SerializedName("signature")
    @Expose
    private String signature;
    @SerializedName("trx")
    @Expose
    private TransactionStructure transactionStructure;
    @SerializedName("trx_id")
    @Expose
    private String trxId;

    @Override
    public String toString() {
        return "BlockStructure{" +
                "id='" + id + '\'' +
                ", account='" + account + '\'' +
                ", operationId='" + operationId + '\'' +
                ", sequence=" + sequence +
                ", next='" + next + '\'' +
                ", op=" + op +
                ", result=" + result +
                ", blockNum=" + blockNum +
                ", trxInBlock=" + trxInBlock +
                ", opInTrx=" + opInTrx +
                ", virtualOp=" + virtualOp +
                ", signature='" + signature + '\'' +
                ", transactionStructure=" + transactionStructure +
                ", trxId='" + trxId + '\'' +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(final String account) {
        this.account = account;
    }

    public String getOperationId() {
        return operationId;
    }

    public void setOperationId(final String operationId) {
        this.operationId = operationId;
    }

    public Integer getSequence() {
        return sequence;
    }

    public void setSequence(final Integer sequence) {
        this.sequence = sequence;
    }

    public String getNext() {
        return next;
    }

    public void setNext(final String next) {
        this.next = next;
    }

    public List<?> getOp() {
        return op;
    }

    public void setOp(final List<?> op) {
        this.op = op;
    }

    public List<?> getResult() {
        return result;
    }

    public void setResult(final List<?> result) {
        this.result = result;
    }

    public Integer getBlockNum() {
        return blockNum;
    }

    public void setBlockNum(final Integer blockNum) {
        this.blockNum = blockNum;
    }

    public Integer getTrxInBlock() {
        return trxInBlock;
    }

    public void setTrxInBlock(final Integer trxInBlock) {
        this.trxInBlock = trxInBlock;
    }

    public Integer getOpInTrx() {
        return opInTrx;
    }

    public void setOpInTrx(final Integer opInTrx) {
        this.opInTrx = opInTrx;
    }

    public Integer getVirtualOp() {
        return virtualOp;
    }

    public void setVirtualOp(final Integer virtualOp) {
        this.virtualOp = virtualOp;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(final String signature) {
        this.signature = signature;
    }

    public TransactionStructure getTrx() {
        return transactionStructure;
    }

    public void setTrx(final TransactionStructure transactionStructure) {
        this.transactionStructure = transactionStructure;
    }

    public String getTrxId() {
        return trxId;
    }

    public void setTrxId(final String trxId) {
        this.trxId = trxId;
    }

}
