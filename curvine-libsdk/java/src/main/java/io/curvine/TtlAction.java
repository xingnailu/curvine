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

import io.curvine.proto.TtlActionProto;

/**
 * TTL动作枚举，对应Rust中的TtlAction
 */
public enum TtlAction {
    NONE(0),
    FREE(1),
    DELETE(2);

    private final int value;

    TtlAction(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    /**
     * 从Protobuf枚举转换
     */
    public static TtlAction fromProto(TtlActionProto proto) {
        switch (proto) {
            case TTL_ACTION_PROTO_NONE:
                return NONE;
            case TTL_ACTION_PROTO_UFS:
                return FREE;
            case TTL_ACTION_PROTO_DELETE:
                return DELETE;
            default:
                throw new IllegalArgumentException("Unknown TtlActionProto: " + proto);
        }
    }

    /**
     * 转换为Protobuf枚举
     */
    public TtlActionProto toProto() {
        switch (this) {
            case NONE:
                return TtlActionProto.TTL_ACTION_PROTO_NONE;
            case FREE:
                return TtlActionProto.TTL_ACTION_PROTO_UFS;
            case DELETE:
                return TtlActionProto.TTL_ACTION_PROTO_DELETE;
            default:
                throw new IllegalArgumentException("Unknown TtlAction: " + this);
        }
    }
}
