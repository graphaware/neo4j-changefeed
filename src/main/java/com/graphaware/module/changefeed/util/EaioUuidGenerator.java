package com.graphaware.module.changefeed.util;

import com.eaio.uuid.UUID;

/**
 * UUID Generator using the UUID library from http://johannburkard.de/software/uuid/
 */
public class EaioUuidGenerator implements UuidGenerator {

    /**
     * {@inheritDoc}
     */
    @Override
    public String generateUuid() {
        UUID uuid = new UUID();
        return uuid.toString();
    }
}
