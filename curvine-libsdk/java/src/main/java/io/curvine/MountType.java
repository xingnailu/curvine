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

import io.curvine.proto.MountTypeProto;

/**
 * 挂载类型枚举，对应Rust中的MountType
 */
public enum MountType {
    CST(0),
    ORCH(1);

    private final int value;

    MountType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    /**
     * 从Protobuf枚举转换
     */
    public static MountType fromProto(MountTypeProto proto) {
        switch (proto) {
            case MOUNT_TYPE_PROTO_CST:
                return CST;
            case MOUNT_TYPE_PROTO_ORCH:
                return ORCH;
            default:
                throw new IllegalArgumentException("Unknown MountTypeProto: " + proto);
        }
    }

    /**
     * 转换为Protobuf枚举
     */
    public MountTypeProto toProto() {
        switch (this) {
            case CST:
                return MountTypeProto.MOUNT_TYPE_PROTO_CST;
            case ORCH:
                return MountTypeProto.MOUNT_TYPE_PROTO_ORCH;
            default:
                throw new IllegalArgumentException("Unknown MountType: " + this);
        }
    }
}
