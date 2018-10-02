package com.zegelin.agent;

import com.google.common.collect.ImmutableList;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public final class AgentArgumentParser {
    AgentArgumentParser() {}

    enum State {
        ARGUMENT,
        QUOTED,
        ESCAPED
    }

    public static List<String> parseArguments(final String argumentsString) {
        if (argumentsString == null) {
            return ImmutableList.of();
        }

        final Deque<State> stateStack = new LinkedList<>();
        final ImmutableList.Builder<String> arguments = ImmutableList.builder();
        final StringBuilder currentArgument = new StringBuilder();

        stateStack.push(State.ARGUMENT);

        final char[] chars = (argumentsString + '\0').toCharArray();

        for (final char c : chars) {
            assert stateStack.peek() != null;

            switch (stateStack.peek()) {
                case ARGUMENT:
                    if (c == ' ' || c == ',' || c == '\0') {
                        if (currentArgument.length() > 0) {
                            arguments.add(currentArgument.toString());
                        }

                        currentArgument.setLength(0);
                        continue;
                    }

                    if (c == '\\') {
                        stateStack.push(State.ESCAPED);
                        continue;
                    }

                    if (c == '"') {
                        stateStack.push(State.QUOTED);
                    }

                    break;

                case QUOTED:
                    if (c == '"') {
                        stateStack.pop();
                    }

                    if (c == '\\') {
                        stateStack.push(State.ESCAPED);
                    }

                    break;

                case ESCAPED:
                    stateStack.pop();

                    break;

                default:
                    throw new IllegalStateException();
            }

            currentArgument.append(c);
        }

        if (stateStack.peek() != State.ARGUMENT) {
            throw new IllegalStateException(String.format("Argument %s is invalid.", currentArgument));
        }

        return arguments.build();
    }
}
