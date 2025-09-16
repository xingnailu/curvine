// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.curvine;

import io.curvine.proto.StorageTypeProto;

/**
 * 存储类型枚举，对应Rust中的StorageType
 */
public enum StorageType {
    DISK(0),
    SSD(1),
    MEMORY(2);

    private final int value;

    StorageType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    /**
     * 从Protobuf枚举转换
     */
    public static StorageType fromProto(StorageTypeProto proto) {
        switch (proto) {
            case STORAGE_TYPE_PROTO_DISK:
                return DISK;
            case STORAGE_TYPE_PROTO_SSD:
                return SSD;
            case STORAGE_TYPE_PROTO_MEM:
                return MEMORY;
            default:
                throw new IllegalArgumentException("Unknown StorageTypeProto: " + proto);
        }
    }

    /**
     * 转换为Protobuf枚举
     */
    public StorageTypeProto toProto() {
        switch (this) {
            case DISK:
                return StorageTypeProto.STORAGE_TYPE_PROTO_DISK;
            case SSD:
                return StorageTypeProto.STORAGE_TYPE_PROTO_SSD;
            case MEMORY:
                return StorageTypeProto.STORAGE_TYPE_PROTO_MEM;
            default:
                throw new IllegalArgumentException("Unknown StorageType: " + this);
        }
    }
}
