package com.graphaware.module.changefeed;

import java.util.Date;
import java.util.Set;

/**
 * A set of changes applied on the graph in a single transaction
 */
public class ChangeSet {

    private Date changeDate;
    private Set<String> changes;

    public ChangeSet() {
        this.changeDate = new Date();
    }

    public Date getChangeDate() {
        return changeDate;
    }

    public void setChangeDate(Date changeDate) {
        this.changeDate = changeDate;
    }

    public Set<String> getChanges() {
        return changes;
    }

    public void setChanges(Set<String> changes) {
        this.changes = changes;
    }
}
