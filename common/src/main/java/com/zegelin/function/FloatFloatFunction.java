package com.zegelin.function;

@FunctionalInterface
public interface FloatFloatFunction {
    float apply(float f);

    static FloatFloatFunction identity() {
        return (f) -> f;
    }
}
