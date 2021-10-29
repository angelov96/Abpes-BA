package graphene.listener;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

public class BlockWrapperWallet {

    @SerializedName("id")
    @Expose
    private Integer id;
    @SerializedName("jsonrpc")
    @Expose
    private String jsonrpc;
    @SerializedName("result")
    @Expose
    private BlockStructureWallet result;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getJsonrpc() {
        return jsonrpc;
    }

    public void setJsonrpc(String jsonrpc) {
        this.jsonrpc = jsonrpc;
    }

    public BlockStructureWallet getResult() {
        return result;
    }

    public void setResult(BlockStructureWallet result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("id", id).append("jsonrpc", jsonrpc).append("result", result).toString();
    }
}
