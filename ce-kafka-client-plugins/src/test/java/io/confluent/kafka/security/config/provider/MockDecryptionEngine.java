/*
 * Copyright 2018 Confluent Inc.
 */

package io.confluent.kafka.security.config.provider;

/**
 * Mocks Decryption Engine.
 */

public class MockDecryptionEngine extends DecryptionEngine {

    public MockDecryptionEngine(String masterKeyEnvr, String dataKeyCipher, String keyLength) throws Exception {
        super(masterKeyEnvr, dataKeyCipher, keyLength);
    }

    @Override
    protected String loadMasterKey(String environmentVariable) {
        // Load the master key from environment variable.
        return "eqDnvdYAUjxuSZvlNQn241xx80RYQJgAdu45+DdIn9E=";

    }

}
