package com.hazelcast.query.extractor;

public abstract class ArgumentsParser<T> {

    public abstract Arguments<T> parse(Object input);

}
