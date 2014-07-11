package com.graphaware.module.changefeed;

import com.graphaware.common.strategy.InclusionStrategies;
import com.graphaware.runtime.config.BaseTxDrivenModuleConfiguration;
import com.graphaware.runtime.strategy.InclusionStrategiesFactory;

/**
 * {@link BaseTxDrivenModuleConfiguration} for {@link ChangeFeedModule}.
 */
public class ChangeFeedConfiguration extends BaseTxDrivenModuleConfiguration<ChangeFeedConfiguration> {

    private static final int MAX_CHANGES_DEFAULT = 50;

    private final int maxChanges;

    /**
     * Create a configuration with maximum number of changes = {@link #MAX_CHANGES_DEFAULT} and
     * inclusion strategies = {@link com.graphaware.runtime.strategy.InclusionStrategiesFactory#allBusiness()}
     * (nothing is excluded except for framework-internal nodes and relationships).
     */
    public ChangeFeedConfiguration() {
        this(MAX_CHANGES_DEFAULT);
    }

    /**
     * Create a configuration with given maximum number of changes and
     * inclusion strategies = {@link com.graphaware.runtime.strategy.InclusionStrategiesFactory#allBusiness()}
     * (nothing is excluded except for framework-internal nodes and relationships).
     *
     * @param maxChanges maximum number of changes to store before some oldest ones are pruned.
     */
    public ChangeFeedConfiguration(int maxChanges) {
        this(InclusionStrategiesFactory.allBusiness(), maxChanges);
    }

    /**
     * Create a configuration with maximum number of changes = {@link #MAX_CHANGES_DEFAULT} and
     * custom inclusion strategies.
     *
     * @param inclusionStrategies defining what to keep track of and which changes to ignore.
     */
    public ChangeFeedConfiguration(InclusionStrategies inclusionStrategies) {
        this(inclusionStrategies, MAX_CHANGES_DEFAULT);
    }

    /**
     * Create a configuration with given maximum number of changes and custom inclusion strategies.
     *
     * @param maxChanges          maximum number of changes to store before some oldest ones are pruned.
     * @param inclusionStrategies defining what to keep track of and which changes to ignore.
     */
    public ChangeFeedConfiguration(InclusionStrategies inclusionStrategies, int maxChanges) {
        super(inclusionStrategies);
        this.maxChanges = maxChanges;
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
     * {@inheritDoc}
     */
    @Override
    protected ChangeFeedConfiguration newInstance(InclusionStrategies inclusionStrategies) {
        return new ChangeFeedConfiguration(inclusionStrategies, getMaxChanges());
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
