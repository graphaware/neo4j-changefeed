/*
 * Copyright (c) 2013-2015 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.changefeed;

import com.graphaware.common.policy.InclusionPolicies;
import com.graphaware.runtime.config.BaseTxAndTimerDrivenModuleConfiguration;
import com.graphaware.runtime.config.BaseTxDrivenModuleConfiguration;
import com.graphaware.runtime.config.TxAndTimerDrivenModuleConfiguration;
import com.graphaware.runtime.policy.InclusionPoliciesFactory;

/**
 * {@link BaseTxDrivenModuleConfiguration} for {@link ChangeFeedModule}.
 */
public class ChangeFeedConfiguration extends BaseTxAndTimerDrivenModuleConfiguration<ChangeFeedConfiguration> implements TxAndTimerDrivenModuleConfiguration {

    private static final int DEFAULT_MAX_CHANGES = 100;
    private static final int DEFAULT_PRUNE_DELAY = 10000;
    private static final int DEFAULT_PRUNE_WHEN_MAX_EXCEEDED_BY = 10;

    private final int maxChanges;
    private final int pruneDelay;
    private final int pruneWhenMaxExceededBy;

    /**
     * Create a default configuration with maximum number of changes = {@link #DEFAULT_MAX_CHANGES},
     * inclusion policies = {@link com.graphaware.runtime.policy.InclusionPoliciesFactory#allBusiness()},
     * (nothing is excluded except for framework-internal nodes and relationships),
     * initialize until = {@link #NEVER} (this module does not do any initialization), instance policy = {@link InstanceRolePolicy#MASTER_ONLY},
     * prune delay = {@link #DEFAULT_PRUNE_DELAY}, and prune when max exceeded by = {@link #DEFAULT_PRUNE_WHEN_MAX_EXCEEDED_BY}.
     * <p/>
     * Change this by calling {@link #withMaxChanges(int)}, {@link #withPruneDelay(int)}, {@link #withPruneWhenMaxExceededBy(int)}, with
     * other inclusion policies on the object, always using the returned object (this is a fluent interface).
     */
    public static ChangeFeedConfiguration defaultConfiguration() {
        return new ChangeFeedConfiguration(InclusionPoliciesFactory.allBusiness(), NEVER, InstanceRolePolicy.MASTER_ONLY, DEFAULT_MAX_CHANGES, DEFAULT_PRUNE_DELAY, DEFAULT_PRUNE_WHEN_MAX_EXCEEDED_BY);
    }

    /**
     * Create a custom configuration.
     *
     * @param inclusionPolicies      defining what to keep track of and which changes to ignore.
     * @param initializeUntil        until what time in ms since epoch it is ok to re(initialize) the entire module in case the configuration
     *                               has changed since the last time the module was started, or if it is the first time the module was registered.
     *                               {@link #NEVER} for never, {@link #ALWAYS} for always.
     * @param instanceRolePolicy     specifies which role a machine must have in order to run the module with this configuration. Must not be <code>null</code>.
     * @param maxChanges             maximum number of changes to store before some oldest ones are pruned.
     * @param pruneDelay             delay in millis between pruning tasks.
     * @param pruneWhenMaxExceededBy number of changes the maximum needs to be exceeded by before the oldest ones are pruned.
     */
    protected ChangeFeedConfiguration(InclusionPolicies inclusionPolicies, long initializeUntil, InstanceRolePolicy instanceRolePolicy, int maxChanges, int pruneDelay, int pruneWhenMaxExceededBy) {
        super(inclusionPolicies, initializeUntil, instanceRolePolicy);
        this.maxChanges = maxChanges;
        this.pruneDelay = pruneDelay;
        this.pruneWhenMaxExceededBy = pruneWhenMaxExceededBy;
    }

    /**
     * Get the maximum number of changes to be maintained in the feed.
     *
     * @return max changes.
     */
    public int getMaxChanges() {
        return maxChanges;
    }

    /**
     * Get the configured delayed for pruning the feed.
     *
     * @return delay in ms.
     */
    public int getPruneDelay() {
        return pruneDelay;
    }

    /**
     * Get the configured number of changes by which the maximum number of changes must be exceeded in order for pruning to take place.
     *
     * @return number of changes.
     */
    public int getPruneWhenMaxExceededBy() {
        return pruneWhenMaxExceededBy;
    }

    /**
     * Create a new instance of this {@link ChangeFeedConfiguration} with different maxChanges.
     *
     * @param maxChanges of the new instance.
     * @return new instance.
     */
    public ChangeFeedConfiguration withMaxChanges(int maxChanges) {
        return new ChangeFeedConfiguration(getInclusionPolicies(), initializeUntil(), getInstanceRolePolicy(), maxChanges, getPruneDelay(), getPruneWhenMaxExceededBy());
    }

    /**
     * Create a new instance of this {@link ChangeFeedConfiguration} with different prune delay.
     *
     * @param pruneDelay of the new instance.
     * @return new instance.
     */
    public ChangeFeedConfiguration withPruneDelay(int pruneDelay) {
        return new ChangeFeedConfiguration(getInclusionPolicies(), initializeUntil(), getInstanceRolePolicy(),getMaxChanges(), pruneDelay, getPruneWhenMaxExceededBy());
    }

    /**
     * Create a new instance of this {@link ChangeFeedConfiguration} with different number of changes the maximum must
     * be exceeded by before pruning takes place.
     *
     * @param pruneWhenMaxExceededBy of the new instance.
     * @return new instance.
     */
    public ChangeFeedConfiguration withPruneWhenMaxExceededBy(int pruneWhenMaxExceededBy) {
        return new ChangeFeedConfiguration(getInclusionPolicies(), initializeUntil(), getInstanceRolePolicy(),getMaxChanges(), pruneDelay, pruneWhenMaxExceededBy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ChangeFeedConfiguration newInstance(InclusionPolicies inclusionPolicies, long initializeUntil, InstanceRolePolicy instanceRolePolicy) {
        return new ChangeFeedConfiguration(inclusionPolicies, initializeUntil, instanceRolePolicy, getMaxChanges(), getPruneDelay(), getPruneWhenMaxExceededBy());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ChangeFeedConfiguration that = (ChangeFeedConfiguration) o;

        if (maxChanges != that.maxChanges) {
            return false;
        }
        if (pruneDelay != that.pruneDelay) {
            return false;
        }
        if (pruneWhenMaxExceededBy != that.pruneWhenMaxExceededBy) {
            return false;
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + maxChanges;
        result = 31 * result + pruneDelay;
        result = 31 * result + pruneWhenMaxExceededBy;
        return result;
    }
}
