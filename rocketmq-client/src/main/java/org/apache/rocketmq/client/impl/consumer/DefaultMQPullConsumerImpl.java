package org.apache.rocketmq.client.impl.consumer;

import apache.rocketmq.v1.ClientResourceBundle;
import apache.rocketmq.v1.ConsumerGroup;
import apache.rocketmq.v1.HeartbeatEntry;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.QueryOffsetResponse;
import apache.rocketmq.v1.Resource;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Metadata;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.constant.ServiceState;
import org.apache.rocketmq.client.consumer.OffsetQuery;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullMessageQuery;
import org.apache.rocketmq.client.consumer.PullMessageResult;
import org.apache.rocketmq.client.consumer.QueryOffsetPolicy;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.exception.ErrorCode;
import org.apache.rocketmq.client.impl.ClientBaseImpl;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.misc.MixAll;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.utility.ThreadFactoryImpl;

@Slf4j
public class DefaultMQPullConsumerImpl extends ClientBaseImpl {


    private final ThreadPoolExecutor pullCallbackExecutor;

    public DefaultMQPullConsumerImpl(String group) {
        super(group);
        this.pullCallbackExecutor = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("PullCallbackThread"));
    }

    public ListenableFuture<List<MessageQueue>> getQueuesFor(String topic) {
        final ListenableFuture<TopicRouteData> future = getRouteFor(topic);
        return Futures.transformAsync(future, new AsyncFunction<TopicRouteData, List<MessageQueue>>() {
            @Override
            public ListenableFuture<List<MessageQueue>> apply(TopicRouteData topicRouteData) throws Exception {
                // TODO: polish code
                List<MessageQueue> messageQueues = new ArrayList<MessageQueue>();
                final List<Partition> partitions = topicRouteData.getPartitions();
                for (Partition partition : partitions) {
                    if (MixAll.MASTER_BROKER_ID != partition.getBroker().getId()) {
                        continue;
                    }
                    if (partition.getPermission().isReadable()) {
                        final MessageQueue messageQueue = new MessageQueue(partition);
                        messageQueues.add(messageQueue);
                    }
                }
                if (messageQueues.isEmpty()) {
                    throw new ClientException(ErrorCode.NO_PERMISSION);
                }
                SettableFuture<List<MessageQueue>> future0 = SettableFuture.create();
                future0.set(messageQueues);
                return future0;
            }
        });
    }

    public QueryOffsetRequest wrapQueryOffsetRequest(OffsetQuery offsetQuery) {
        final QueryOffsetRequest.Builder builder = QueryOffsetRequest.newBuilder();
        final QueryOffsetPolicy queryOffsetPolicy = offsetQuery.getQueryOffsetPolicy();
        switch (queryOffsetPolicy) {
            case END:
                builder.setPolicy(apache.rocketmq.v1.QueryOffsetPolicy.END);
                break;
            case TIME_POINT:
                builder.setPolicy(apache.rocketmq.v1.QueryOffsetPolicy.TIME_POINT);
                builder.setTimePoint(Timestamps.fromMillis(offsetQuery.getTimePoint()));
                break;
            case BEGINNING:
            default:
                builder.setPolicy(apache.rocketmq.v1.QueryOffsetPolicy.BEGINNING);
        }
        final MessageQueue messageQueue = offsetQuery.getMessageQueue();
        Resource topicResource = Resource.newBuilder().setArn(this.getArn()).setName(messageQueue.getTopic()).build();
        int partitionId = messageQueue.getPartition().getId();
        final apache.rocketmq.v1.Partition partition = apache.rocketmq.v1.Partition.newBuilder()
                                                                                   .setTopic(topicResource)
                                                                                   .setId(partitionId)
                                                                                   .build();
        return QueryOffsetRequest.newBuilder().setPartition(partition).build();
    }

    public ListenableFuture<Long> queryOffset(final OffsetQuery offsetQuery) {
        final QueryOffsetRequest request = wrapQueryOffsetRequest(offsetQuery);
        final SettableFuture<Long> future0 = SettableFuture.create();
        Metadata metadata;
        try {
            metadata = sign();
        } catch (Throwable t) {
            future0.setException(t);
            return future0;
        }
        final Partition partition = offsetQuery.getMessageQueue().getPartition();
        final Endpoints endpoints = partition.getBroker().getEndpoints();
        final ListenableFuture<QueryOffsetResponse> future =
                clientInstance.queryOffset(endpoints, metadata, request, ioTimeoutMillis, TimeUnit.MILLISECONDS);
        return Futures.transformAsync(future, new AsyncFunction<QueryOffsetResponse, Long>() {
            @Override
            public ListenableFuture<Long> apply(QueryOffsetResponse response) throws Exception {
                final Status status = response.getCommon().getStatus();
                final Code code = Code.forNumber(status.getCode());
                // TODO: polish code.
                if (Code.OK != code) {
                    log.error("Failed to query offset, offsetQuery={}, code={}, message={}", offsetQuery, code,
                              status.getMessage());
                    throw new ClientException(ErrorCode.OTHER);
                }
                final long offset = response.getOffset();
                future0.set(offset);
                return future0;
            }
        });
    }



    public void pull(PullMessageQuery pullMessageQuery, final PullCallback pullCallback) {
        final ListenableFuture<PullMessageResult> future = pull(pullMessageQuery);
        Futures.addCallback(future, new FutureCallback<PullMessageResult>() {
            @Override
            public void onSuccess(final PullMessageResult pullMessageResult) {
                try {
                    pullCallbackExecutor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                pullCallback.onSuccess(pullMessageResult);
                            } catch (Throwable t) {
                                log.error("Exception occurs in PullCallback#onSuccess", t);
                            }
                        }
                    });
                } catch (Throwable t) {
                    log.error("Exception occurs while submitting task to pull callback executor", t);
                }
            }

            @Override
            public void onFailure(final Throwable t) {
                try {
                    pullCallbackExecutor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                pullCallback.onException(t);
                            } catch (Throwable t) {
                                log.error("Exception occurs in PullCallback#onException", t);
                            }
                        }
                    });
                } catch (Throwable t0) {
                    log.error("Exception occurs while submitting task to pull callback executor", t0);
                }
            }
        });
    }

    @Override
    public void start() throws ClientException {
        synchronized (this) {
            log.warn("Begin to start the rocketmq pull consumer.");
            super.start();
            if (ServiceState.STARTED == getState()) {
                log.info("The rocketmq pull consumer starts successfully.");
            }
        }
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            log.info("Begin to shutdown the rocketmq pull consumer.");
            super.shutdown();
            if (ServiceState.STOPPED == getState()) {
                pullCallbackExecutor.shutdown();
                log.info("Shutdown the rocketmq pull consumer successfully.");
            }
        }
    }


    @Override
    public HeartbeatEntry prepareHeartbeatData() {
        final Resource groupResource =
                Resource.newBuilder().setArn(this.getArn()).setName(group).build();
        final ConsumerGroup consumerGroup = ConsumerGroup.newBuilder().setGroup(groupResource).build();
        return HeartbeatEntry.newBuilder().setClientId(clientId)
                             .setConsumerGroup(consumerGroup).build();
    }

    @Override
    public void doStats() {

    }

    @Override
    public ClientResourceBundle wrapClientResourceBundle() {
        Resource groupResource = Resource.newBuilder().setArn(arn).setName(group).build();
        final ClientResourceBundle.Builder builder =
                ClientResourceBundle.newBuilder().setClientId(clientId).setProducerGroup(groupResource);
        return builder.build();
    }


}



