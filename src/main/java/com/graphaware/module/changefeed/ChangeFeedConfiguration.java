package com.graphaware.module.changefeed;

import com.graphaware.common.strategy.InclusionStrategies;
import com.graphaware.runtime.config.BaseTxDrivenModuleConfiguration;
import com.graphaware.runtime.config.TxDrivenModuleConfiguration;
import com.graphaware.runtime.strategy.InclusionStrategiesFactory;

/**
 *  {@link TxDrivenModuleConfiguration} for {@link ChangeFeedModule}.
 */
public class ChangeFeedConfiguration extends BaseTxDrivenModuleConfiguration<ChangeFeedConfiguration> {

    private static final int MAX_CHANGES_DEFAULT = 50;

    private final int maxChanges;

    public ChangeFeedConfiguration() {
        this(MAX_CHANGES_DEFAULT);
    }

    public ChangeFeedConfiguration(int maxChanges) {
        this(InclusionStrategiesFactory.allBusiness(), maxChanges);
    }

    public ChangeFeedConfiguration(InclusionStrategies inclusionStrategies) {
        this(inclusionStrategies, MAX_CHANGES_DEFAULT);
    }

    public ChangeFeedConfiguration(InclusionStrategies inclusionStrategies, int maxChanges) {
        super(inclusionStrategies);
        this.maxChanges = maxChanges;
    }

    /**
     * Get the maximum number of changes to be maintained in the feed
     *
     * @return max changes
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
}
