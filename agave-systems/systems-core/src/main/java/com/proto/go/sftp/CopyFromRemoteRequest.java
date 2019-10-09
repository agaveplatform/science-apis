// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: src/proto/sftp.proto

package com.proto.go.sftp;

/**
 * Protobuf type {@code sftp.CopyFromRemoteRequest}
 */
public  final class CopyFromRemoteRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:sftp.CopyFromRemoteRequest)
    CopyFromRemoteRequestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use CopyFromRemoteRequest.newBuilder() to construct.
  private CopyFromRemoteRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private CopyFromRemoteRequest() {
  }

  @Override
  @SuppressWarnings({"unused"})
  protected Object newInstance(
      UnusedPrivateParameter unused) {
    return new CopyFromRemoteRequest();
  }

  @Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private CopyFromRemoteRequest(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    if (extensionRegistry == null) {
      throw new NullPointerException();
    }
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          case 10: {
            Sftp.Builder subBuilder = null;
            if (sftp_ != null) {
              subBuilder = sftp_.toBuilder();
            }
            sftp_ = input.readMessage(Sftp.parser(), extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(sftp_);
              sftp_ = subBuilder.buildPartial();
            }

            break;
          }
          default: {
            if (!parseUnknownField(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return SftpOuterClass.internal_static_sftp_CopyFromRemoteRequest_descriptor;
  }

  @Override
  protected FieldAccessorTable
      internalGetFieldAccessorTable() {
    return SftpOuterClass.internal_static_sftp_CopyFromRemoteRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            CopyFromRemoteRequest.class, Builder.class);
  }

  public static final int SFTP_FIELD_NUMBER = 1;
  private Sftp sftp_;
  /**
   * <code>.sftp.Sftp sftp = 1;</code>
   */
  public boolean hasSftp() {
    return sftp_ != null;
  }
  /**
   * <code>.sftp.Sftp sftp = 1;</code>
   */
  public Sftp getSftp() {
    return sftp_ == null ? Sftp.getDefaultInstance() : sftp_;
  }
  /**
   * <code>.sftp.Sftp sftp = 1;</code>
   */
  public SftpOrBuilder getSftpOrBuilder() {
    return getSftp();
  }

  private byte memoizedIsInitialized = -1;
  @Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  @Override
  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (sftp_ != null) {
      output.writeMessage(1, getSftp());
    }
    unknownFields.writeTo(output);
  }

  @Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (sftp_ != null) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, getSftp());
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof CopyFromRemoteRequest)) {
      return super.equals(obj);
    }
    CopyFromRemoteRequest other = (CopyFromRemoteRequest) obj;

    if (hasSftp() != other.hasSftp()) return false;
    if (hasSftp()) {
      if (!getSftp()
          .equals(other.getSftp())) return false;
    }
    if (!unknownFields.equals(other.unknownFields)) return false;
    return true;
  }

  @Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    if (hasSftp()) {
      hash = (37 * hash) + SFTP_FIELD_NUMBER;
      hash = (53 * hash) + getSftp().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static CopyFromRemoteRequest parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static CopyFromRemoteRequest parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static CopyFromRemoteRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static CopyFromRemoteRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static CopyFromRemoteRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static CopyFromRemoteRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static CopyFromRemoteRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static CopyFromRemoteRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static CopyFromRemoteRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static CopyFromRemoteRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static CopyFromRemoteRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static CopyFromRemoteRequest parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  @Override
  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(CopyFromRemoteRequest prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  @Override
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @Override
  protected Builder newBuilderForType(
      BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code sftp.CopyFromRemoteRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:sftp.CopyFromRemoteRequest)
      com.proto.go.sftp.CopyFromRemoteRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return SftpOuterClass.internal_static_sftp_CopyFromRemoteRequest_descriptor;
    }

    @Override
    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return SftpOuterClass.internal_static_sftp_CopyFromRemoteRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              CopyFromRemoteRequest.class, Builder.class);
    }

    // Construct using com.proto.go.sftp.CopyFromRemoteRequest.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
      }
    }
    @Override
    public Builder clear() {
      super.clear();
      if (sftpBuilder_ == null) {
        sftp_ = null;
      } else {
        sftp_ = null;
        sftpBuilder_ = null;
      }
      return this;
    }

    @Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return SftpOuterClass.internal_static_sftp_CopyFromRemoteRequest_descriptor;
    }

    @Override
    public CopyFromRemoteRequest getDefaultInstanceForType() {
      return CopyFromRemoteRequest.getDefaultInstance();
    }

    @Override
    public CopyFromRemoteRequest build() {
      CopyFromRemoteRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @Override
    public CopyFromRemoteRequest buildPartial() {
      CopyFromRemoteRequest result = new CopyFromRemoteRequest(this);
      if (sftpBuilder_ == null) {
        result.sftp_ = sftp_;
      } else {
        result.sftp_ = sftpBuilder_.build();
      }
      onBuilt();
      return result;
    }

    @Override
    public Builder clone() {
      return super.clone();
    }
    @Override
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return super.setField(field, value);
    }
    @Override
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return super.clearField(field);
    }
    @Override
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return super.clearOneof(oneof);
    }
    @Override
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, Object value) {
      return super.setRepeatedField(field, index, value);
    }
    @Override
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return super.addRepeatedField(field, value);
    }
    @Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof CopyFromRemoteRequest) {
        return mergeFrom((CopyFromRemoteRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(CopyFromRemoteRequest other) {
      if (other == CopyFromRemoteRequest.getDefaultInstance()) return this;
      if (other.hasSftp()) {
        mergeSftp(other.getSftp());
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    @Override
    public final boolean isInitialized() {
      return true;
    }

    @Override
    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      CopyFromRemoteRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (CopyFromRemoteRequest) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private Sftp sftp_;
    private com.google.protobuf.SingleFieldBuilderV3<
        Sftp, Sftp.Builder, SftpOrBuilder> sftpBuilder_;
    /**
     * <code>.sftp.Sftp sftp = 1;</code>
     */
    public boolean hasSftp() {
      return sftpBuilder_ != null || sftp_ != null;
    }
    /**
     * <code>.sftp.Sftp sftp = 1;</code>
     */
    public Sftp getSftp() {
      if (sftpBuilder_ == null) {
        return sftp_ == null ? Sftp.getDefaultInstance() : sftp_;
      } else {
        return sftpBuilder_.getMessage();
      }
    }
    /**
     * <code>.sftp.Sftp sftp = 1;</code>
     */
    public Builder setSftp(Sftp value) {
      if (sftpBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        sftp_ = value;
        onChanged();
      } else {
        sftpBuilder_.setMessage(value);
      }

      return this;
    }
    /**
     * <code>.sftp.Sftp sftp = 1;</code>
     */
    public Builder setSftp(
        Sftp.Builder builderForValue) {
      if (sftpBuilder_ == null) {
        sftp_ = builderForValue.build();
        onChanged();
      } else {
        sftpBuilder_.setMessage(builderForValue.build());
      }

      return this;
    }
    /**
     * <code>.sftp.Sftp sftp = 1;</code>
     */
    public Builder mergeSftp(Sftp value) {
      if (sftpBuilder_ == null) {
        if (sftp_ != null) {
          sftp_ =
            Sftp.newBuilder(sftp_).mergeFrom(value).buildPartial();
        } else {
          sftp_ = value;
        }
        onChanged();
      } else {
        sftpBuilder_.mergeFrom(value);
      }

      return this;
    }
    /**
     * <code>.sftp.Sftp sftp = 1;</code>
     */
    public Builder clearSftp() {
      if (sftpBuilder_ == null) {
        sftp_ = null;
        onChanged();
      } else {
        sftp_ = null;
        sftpBuilder_ = null;
      }

      return this;
    }
    /**
     * <code>.sftp.Sftp sftp = 1;</code>
     */
    public Sftp.Builder getSftpBuilder() {
      
      onChanged();
      return getSftpFieldBuilder().getBuilder();
    }
    /**
     * <code>.sftp.Sftp sftp = 1;</code>
     */
    public SftpOrBuilder getSftpOrBuilder() {
      if (sftpBuilder_ != null) {
        return sftpBuilder_.getMessageOrBuilder();
      } else {
        return sftp_ == null ?
            Sftp.getDefaultInstance() : sftp_;
      }
    }
    /**
     * <code>.sftp.Sftp sftp = 1;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        Sftp, Sftp.Builder, SftpOrBuilder>
        getSftpFieldBuilder() {
      if (sftpBuilder_ == null) {
        sftpBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            Sftp, Sftp.Builder, SftpOrBuilder>(
                getSftp(),
                getParentForChildren(),
                isClean());
        sftp_ = null;
      }
      return sftpBuilder_;
    }
    @Override
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    @Override
    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:sftp.CopyFromRemoteRequest)
  }

  // @@protoc_insertion_point(class_scope:sftp.CopyFromRemoteRequest)
  private static final CopyFromRemoteRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new CopyFromRemoteRequest();
  }

  public static CopyFromRemoteRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<CopyFromRemoteRequest>
      PARSER = new com.google.protobuf.AbstractParser<CopyFromRemoteRequest>() {
    @Override
    public CopyFromRemoteRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new CopyFromRemoteRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<CopyFromRemoteRequest> parser() {
    return PARSER;
  }

  @Override
  public com.google.protobuf.Parser<CopyFromRemoteRequest> getParserForType() {
    return PARSER;
  }

  @Override
  public CopyFromRemoteRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

