package sawtooth.rest;

import okhttp3.RequestBody;
import retrofit2.Call;
import retrofit2.http.*;
import sawtooth.endpointdata.batch_status.BatchStatus;
import sawtooth.endpointdata.batches_post.Batch;
import sawtooth.endpointdata.blocks.Block;
import sawtooth.endpointdata.receipts.Receipt;
import sawtooth.endpointdata.singlebatch.SingleBatch;
import sawtooth.endpointdata.singleblock.SingleBlock;
import sawtooth.endpointdata.singlestate.SingleState;
import sawtooth.endpointdata.singletransaction.SingleTransaction;
import sawtooth.endpointdata.state.State;
import sawtooth.endpointdata.transactions.Transaction;

import java.util.List;

public interface ISawtoothClientRest {
    @POST("/batches")
    Call<Batch> postBatchList(@Body RequestBody payload);

    @GET("/batches")
    Call<sawtooth.endpointdata.batches.Batch> getBatches(
            @Query("head") String head,
            @Query("start") String start,
            @Query("limit") Integer limit,
            @Query("reverse") String reverse);

    @GET("/batches/{batch_id}")
    Call<SingleBatch> getBatch(
            @Path("batch_id") String batchId);

    @GET("/batch_statuses")
    Call<BatchStatus> getBatchStatus(@Query("id") List<String> ids, @Query("wait") Integer timeout);

    @POST("/batch_statuses")
    Call<BatchStatus> getBatchStatus(@Body RequestBody payload, @Query("wait") Integer timeout);

    @GET("/state")
    Call<State> getState(
            @Query("head") String head,
            @Query("address") String partialAddress,
            @Query("start") String start,
            @Query("limit") Integer limit,
            @Query("reverse") String reverse
    );

    @GET("/state/{address}")
    Call<SingleState> getState(
            @Path("address") String address, @Query("head") String head);

    @GET("/blocks")
    Call<Block> getBlocks(
            @Query("head") String head,
            @Query("start") String start,
            @Query("limit") Integer limit,
            @Query("reverse") String reverse);

    @GET("/blocks/{block_id}")
    Call<SingleBlock> getBlock(
            @Path("block_id") String blockId);

    @GET("/transactions")
    Call<Transaction> getTransactions(
            @Query("head") String head,
            @Query("start") String start,
            @Query("limit") Integer limit,
            @Query("reverse") String reverse);

    @GET("/transactions/{transaction_id}")
    Call<SingleTransaction> getTransaction(
            @Path("transaction_id") String transactionId);

    @GET("/receipts")
    Call<Receipt> getReceipts(
            @Query("id") List<String> ids
    );

    @POST("/receipts")
    Call<Receipt> getReceipts(
            @Body RequestBody payload, @Query("wait") Integer timeout
    );

}