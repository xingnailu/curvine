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

import io.curvine.proto.ConsistencyStrategyProto;

/**
 * 一致性策略枚举，对应Rust中的ConsistencyStrategy
 */
public enum ConsistencyStrategy {
    NONE(0),
    ALWAYS(1);

    private final int value;

    ConsistencyStrategy(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    /**
     * 从Protobuf枚举转换
     */
    public static ConsistencyStrategy fromProto(ConsistencyStrategyProto proto) {
        switch (proto) {
            case CONSISTENCY_STRATEGY_PROTO_NONE:
                return NONE;
            case CONSISTENCY_STRATEGY_PROTO_ALWAYS:
                return ALWAYS;
            default:
                throw new IllegalArgumentException("Unknown ConsistencyStrategyProto: " + proto);
        }
    }

    /**
     * 转换为Protobuf枚举
     */
    public ConsistencyStrategyProto toProto() {
        switch (this) {
            case NONE:
                return ConsistencyStrategyProto.CONSISTENCY_STRATEGY_PROTO_NONE;
            case ALWAYS:
                return ConsistencyStrategyProto.CONSISTENCY_STRATEGY_PROTO_ALWAYS;
            default:
                throw new IllegalArgumentException("Unknown ConsistencyStrategy: " + this);
        }
    }
}
