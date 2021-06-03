package io.appform.dropwizard.actors.base;

import io.appform.dropwizard.actors.base.utils.ConsumerMeta;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class ConnectionMeta<T> {

    private Map<T, List<ConsumerMeta>> consumerMeta = new HashMap<>();
}
