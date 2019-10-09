package com.proto.go.sftp;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import com.proto.go.sftp.SftpOuterClass;
import com.proto.go.sftp.Sftp;
import com.proto.go.sftp.SFTPGrpc;
import com.google.protobuf.Descriptors;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.22.1)",
    comments = "Source: sftp.proto")
public final class SFTPGrpc {

  private SFTPGrpc() {}

  public static final String SERVICE_NAME = "sftp.SFTP";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<CopyFromRemoteRequest,
      CopyFromRemoteResponse> getCopyFromRemoteServiceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CopyFromRemoteService",
      requestType = CopyFromRemoteRequest.class,
      responseType = CopyFromRemoteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<CopyFromRemoteRequest,
      CopyFromRemoteResponse> getCopyFromRemoteServiceMethod() {
    io.grpc.MethodDescriptor<CopyFromRemoteRequest, CopyFromRemoteResponse> getCopyFromRemoteServiceMethod;
    if ((getCopyFromRemoteServiceMethod = SFTPGrpc.getCopyFromRemoteServiceMethod) == null) {
      synchronized (SFTPGrpc.class) {
        if ((getCopyFromRemoteServiceMethod = SFTPGrpc.getCopyFromRemoteServiceMethod) == null) {
          SFTPGrpc.getCopyFromRemoteServiceMethod = getCopyFromRemoteServiceMethod = 
              io.grpc.MethodDescriptor.<CopyFromRemoteRequest, CopyFromRemoteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "sftp.SFTP", "CopyFromRemoteService"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  CopyFromRemoteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  CopyFromRemoteResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new SFTPMethodDescriptorSupplier("CopyFromRemoteService"))
                  .build();
          }
        }
     }
     return getCopyFromRemoteServiceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<CopyLocalToRemoteRequest,
      CopyLocalToRemoteResponse> getCopyLocalToRemoteServiceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CopyLocalToRemoteService",
      requestType = CopyLocalToRemoteRequest.class,
      responseType = CopyLocalToRemoteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<CopyLocalToRemoteRequest,
      CopyLocalToRemoteResponse> getCopyLocalToRemoteServiceMethod() {
    io.grpc.MethodDescriptor<CopyLocalToRemoteRequest, CopyLocalToRemoteResponse> getCopyLocalToRemoteServiceMethod;
    if ((getCopyLocalToRemoteServiceMethod = SFTPGrpc.getCopyLocalToRemoteServiceMethod) == null) {
      synchronized (SFTPGrpc.class) {
        if ((getCopyLocalToRemoteServiceMethod = SFTPGrpc.getCopyLocalToRemoteServiceMethod) == null) {
          SFTPGrpc.getCopyLocalToRemoteServiceMethod = getCopyLocalToRemoteServiceMethod = 
              io.grpc.MethodDescriptor.<CopyLocalToRemoteRequest, CopyLocalToRemoteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "sftp.SFTP", "CopyLocalToRemoteService"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  CopyLocalToRemoteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  CopyLocalToRemoteResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new SFTPMethodDescriptorSupplier("CopyLocalToRemoteService"))
                  .build();
          }
        }
     }
     return getCopyLocalToRemoteServiceMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static SFTPStub newStub(io.grpc.Channel channel) {
    return new SFTPStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static SFTPBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new SFTPBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static SFTPFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new SFTPFutureStub(channel);
  }

  /**
   */
  public static abstract class SFTPImplBase implements io.grpc.BindableService {

    /**
     */
    public void copyFromRemoteService(CopyFromRemoteRequest request,
        io.grpc.stub.StreamObserver<CopyFromRemoteResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCopyFromRemoteServiceMethod(), responseObserver);
    }

    /**
     */
    public void copyLocalToRemoteService(CopyLocalToRemoteRequest request,
        io.grpc.stub.StreamObserver<CopyLocalToRemoteResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCopyLocalToRemoteServiceMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCopyFromRemoteServiceMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                CopyFromRemoteRequest,
                CopyFromRemoteResponse>(
                  this, METHODID_COPY_FROM_REMOTE_SERVICE)))
          .addMethod(
            getCopyLocalToRemoteServiceMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                CopyLocalToRemoteRequest,
                CopyLocalToRemoteResponse>(
                  this, METHODID_COPY_LOCAL_TO_REMOTE_SERVICE)))
          .build();
    }
  }

  /**
   */
  public static final class SFTPStub extends io.grpc.stub.AbstractStub<SFTPStub> {
    private SFTPStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SFTPStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SFTPStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SFTPStub(channel, callOptions);
    }

    /**
     */
    public void copyFromRemoteService(CopyFromRemoteRequest request,
        io.grpc.stub.StreamObserver<CopyFromRemoteResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCopyFromRemoteServiceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void copyLocalToRemoteService(CopyLocalToRemoteRequest request,
        io.grpc.stub.StreamObserver<CopyLocalToRemoteResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCopyLocalToRemoteServiceMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class SFTPBlockingStub extends io.grpc.stub.AbstractStub<SFTPBlockingStub> {
    private SFTPBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SFTPBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SFTPBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SFTPBlockingStub(channel, callOptions);
    }

    /**
     */
    public CopyFromRemoteResponse copyFromRemoteService(CopyFromRemoteRequest request) {
      return blockingUnaryCall(
          getChannel(), getCopyFromRemoteServiceMethod(), getCallOptions(), request);
    }

    /**
     */
    public CopyLocalToRemoteResponse copyLocalToRemoteService(CopyLocalToRemoteRequest request) {
      return blockingUnaryCall(
          getChannel(), getCopyLocalToRemoteServiceMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class SFTPFutureStub extends io.grpc.stub.AbstractStub<SFTPFutureStub> {
    private SFTPFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private SFTPFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected SFTPFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new SFTPFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<CopyFromRemoteResponse> copyFromRemoteService(
        CopyFromRemoteRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCopyFromRemoteServiceMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<CopyLocalToRemoteResponse> copyLocalToRemoteService(
        CopyLocalToRemoteRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCopyLocalToRemoteServiceMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_COPY_FROM_REMOTE_SERVICE = 0;
  private static final int METHODID_COPY_LOCAL_TO_REMOTE_SERVICE = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final SFTPImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(SFTPImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_COPY_FROM_REMOTE_SERVICE:
          serviceImpl.copyFromRemoteService((CopyFromRemoteRequest) request,
              (io.grpc.stub.StreamObserver<CopyFromRemoteResponse>) responseObserver);
          break;
        case METHODID_COPY_LOCAL_TO_REMOTE_SERVICE:
          serviceImpl.copyLocalToRemoteService((CopyLocalToRemoteRequest) request,
              (io.grpc.stub.StreamObserver<CopyLocalToRemoteResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class SFTPBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    SFTPBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return SftpOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("SFTP");
    }
  }

  private static final class SFTPFileDescriptorSupplier
      extends SFTPBaseDescriptorSupplier {
    SFTPFileDescriptorSupplier() {}
  }

  private static final class SFTPMethodDescriptorSupplier
      extends SFTPBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    SFTPMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (SFTPGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new SFTPFileDescriptorSupplier())
              .addMethod(getCopyFromRemoteServiceMethod())
              .addMethod(getCopyLocalToRemoteServiceMethod())
              .build();
        }
      }
    }
    return result;
  }
}
