package batchmode.data;

import java.util.HashMap;

/**
 *
 */
public class DocumentUpdate implements Comparable<DocumentUpdate>{
    String oldJobTitle, newJobTitle, oldIndustry, newIndustry;
    long version;

    public DocumentUpdate(String oldJobTitle, String newJobTitle, String oldIndustry, String newIndustry, long version) {
        this.oldJobTitle = oldJobTitle;
        this.newJobTitle = newJobTitle;
        this.oldIndustry = oldIndustry;
        this.newIndustry = newIndustry;
        this.version = version;
    }

    /**
     * Constructs a new DocumentUpdate object from a <code>HashMap</code> of changes. The given <code>HashMap</code> should
     * contain <code>String</code> values associated with at least some of the keys <code>"old_job_title"</code>,
     * <code>"new_job_title"</code>, <code>"old_industry"</code>, and <code>"new_industry"</code>.
     * @param changes a HashMap of changes such as may be published by the DocumentUpdateQueue
     */
    public DocumentUpdate(HashMap<String, Object> changes) {
        this.oldJobTitle = (String) changes.get("old_job_title");
        this.newJobTitle = (String) changes.get("new_job_title");
        this.oldIndustry = (String) changes.get("old_industry");
        this.newIndustry = (String) changes.get("new_industry");
        //parseLong and toString is chosen here over (Long) cast because this is more robust
        this.version = Long.parseLong(changes.get("version").toString());
    }

    /**
     * Determines whether this DocumentUpdate represents a change in job_title.
     * @return <code>true</code> if the <code>oldJobTitle</code> and <code>newJobTitle</code> fields are not null;
     * <code>false</code> otherwise
     */
    public boolean isJobTitleChange() {
        return (oldJobTitle != null && newJobTitle != null);
    }
    /**
     * Determines whether this DocumentUpdate represents a change in job_title.
     * @return <code>true</code> if the <code>oldIndustry</code> and <code>newIndustry</code> fields are not null;
     * <code>false</code> otherwise
     */
    public boolean isIndustryChange() {
        return (oldIndustry != null && newIndustry != null);
    }
    /**
     * Determines whether this DocumentUpdate represents a new document being inserted
     * @return <code>true</code> if the <code>oldIndustry</code> and <code>oldJobTitle</code> are null and the
     * <code>newIndustry</code> and <code>newJobTitle</code> fields are not null; <code>false</code> otherwise
     */
    public boolean isInsert() {
        return (oldIndustry==null && oldJobTitle==null && newIndustry!=null && newJobTitle!=null);
    }
    /**
     * Determines whether this DocumentUpdate represents a document being removed
     * @return <code>true</code> if the <code>newIndustry</code> and <code>newJobTitle</code> are null and the
     * <code>oldIndustry</code> and <code>oldJobTitle</code> fields are not null; <code>false</code> otherwise
     */
    public boolean isDelete() {
        return (oldIndustry!=null && oldJobTitle!=null && newIndustry==null && newJobTitle==null);
    }

    /**
     * Returns the old job title
     * @return old job title
     */
    public String getOldJobTitle() {
        return oldJobTitle;
    }
    /**
     * Returns the new job title
     * @return new job title
     */
    public String getNewJobTitle() {
        return newJobTitle;
    }
    /**
     * Returns the old industry
     * @return old industry
     */
    public String getOldIndustry() {
        return oldIndustry;
    }
    /**
     * Returns the new industry
     * @return new industry
     */
    public String getNewIndustry() {
        return newIndustry;
    }

    /**
     * Returns the version number
     * @return the version number
     */
    public long getVersion() {
        return version;
    }

    /**
     * Compares this <code>DocumentUpdate</code> with the specified <code>DocumentUpdate</code> for order.  Ordering
     * is based on
     * @param o
     * @return
     */
    @Override
    public int compareTo(DocumentUpdate o) {
        return (int) (this.getVersion()-o.getVersion());
    }
}
