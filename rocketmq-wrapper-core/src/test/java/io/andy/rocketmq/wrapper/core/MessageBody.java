package io.andy.rocketmq.wrapper.core;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class MessageBody {
    private String content;
}
