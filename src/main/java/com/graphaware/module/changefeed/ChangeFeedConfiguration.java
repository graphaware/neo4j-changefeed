package com.graphaware.module.changefeed;

import com.graphaware.common.strategy.InclusionStrategies;
import com.graphaware.runtime.config.BaseTxDrivenModuleConfiguration;
import com.graphaware.runtime.strategy.InclusionStrategiesFactory;

/**
 * {@link BaseTxDrivenModuleConfiguration} for {@link ChangeFeedModule}.
 */
public class ChangeFeedConfiguration extends BaseTxDrivenModuleConfiguration<ChangeFeedConfiguration> {

    private static final int MAX_CHANGES_DEFAULT = 100;
    private static final int DEFAULT_PRUNE_DELAY = 10000;

    private final int maxChanges;
    private final int pruneDelay;

    /**
     * Create a default configuration with maximum number of changes = {@link #MAX_CHANGES_DEFAULT},
     * inclusion strategies = {@link com.graphaware.runtime.strategy.InclusionStrategiesFactory#allBusiness()},
     * (nothing is excluded except for framework-internal nodes and relationships), and prune delay = {@link #DEFAULT_PRUNE_DELAY}.
     * <p/>
     * Change this by calling {@link #withMaxChanges(int)}, {@link #withPruneDelay(int)}, with* other inclusion strategies
     * on the object, always using the returned object (this is a fluent interface).
     */
    public static ChangeFeedConfiguration defaultConfiguration() {
        return new ChangeFeedConfiguration(InclusionStrategiesFactory.allBusiness(), MAX_CHANGES_DEFAULT, DEFAULT_PRUNE_DELAY);
    }

    /**
     * Create a configuration with given maximum number of changes and custom inclusion strategies.
     *
     * @param inclusionStrategies defining what to keep track of and which changes to ignore.
     * @param maxChanges          maximum number of changes to store before some oldest ones are pruned.
     * @param pruneDelay          delay in millis between pruning tasks.
     */
    protected ChangeFeedConfiguration(InclusionStrategies inclusionStrategies, int maxChanges, int pruneDelay) {
        super(inclusionStrategies);
        this.maxChanges = maxChanges;
        this.pruneDelay = pruneDelay;
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
     * Create a new instance of this {@link ChangeFeedConfiguration} with different maxChanges.
     *
     * @param maxChanges of the new instance.
     * @return new instance.
     */
    public ChangeFeedConfiguration withMaxChanges(int maxChanges) {
        return new ChangeFeedConfiguration(getInclusionStrategies(), maxChanges, getPruneDelay());
    }

    /**
     * Create a new instance of this {@link ChangeFeedConfiguration} with different prune delay.
     *
     * @param pruneDelay of the new instance.
     * @return new instance.
     */
    public ChangeFeedConfiguration withPruneDelay(int pruneDelay) {
        return new ChangeFeedConfiguration(getInclusionStrategies(), getMaxChanges(), pruneDelay);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ChangeFeedConfiguration newInstance(InclusionStrategies inclusionStrategies) {
        return new ChangeFeedConfiguration(inclusionStrategies, getMaxChanges(), getPruneDelay());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ChangeFeedConfiguration that = (ChangeFeedConfiguration) o;

        if (maxChanges != that.maxChanges) return false;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + maxChanges;
        return result;
    }
}
