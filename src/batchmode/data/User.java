package batchmode.data;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * The batchmode.data.User object is a plain java object that is designed to
 */
public class User {
    private String name;
    private String jobTitle;
    private String industry;
    private long version;

    /**
     * Constructs a new <code>User</code> with the given name, job title, industry. This user's version number is 1.
     */
    public User(String name, String jobTitle, String industry) {
        this.name = name;
        this.jobTitle = jobTitle;
        this.industry = industry;
        this.version = 1l;
    }
    /**
     * Constructs a new <code>User</code> with the given name, job title, industry, and version number.
     */
    public User(String name, String jobTitle, String industry, long version) {
        this.name = name;
        this.jobTitle = jobTitle;
        this.industry = industry;
        this.version = version;
    }

    /**
     * Constructs a new <code>User</code> from the given json string. Values for "name", "job_title", "industry", and
     * "version" fields will be parsed into their appropriate fields. If the given json does not contain a value for
     * "version", then version will be set to 1.
     * @param json
     */
    public User(String json) {
        JSONObject obj = (JSONObject) JSONValue.parse(json);
        String key = "name";
        this.name = obj.get("name").toString();
        this.jobTitle = obj.get("job_title").toString();
        this.industry = obj.get("industry").toString();

        Object versionObj = obj.get("version");
        this.version = (versionObj!=null) ? Long.parseLong(versionObj.toString()) : 1l;
    }

    /**
     * Returns a JSONObject representing this User
     * @return a JSONObject representation of this User
     */
    public JSONObject getAsJSONObject() {
        JSONObject obj = new JSONObject();
        obj.put("name", name);
        obj.put("job_title", jobTitle);
        obj.put("industry", industry);
        obj.put("version", version);
        return obj;
    }

    /* Getters and Setters */

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJobTitle() {
        return jobTitle;
    }

    public void setJobTitle(String jobTitle) {
        this.jobTitle = jobTitle;
    }

    public String getIndustry() {
        return industry;
    }

    public void setIndustry(String industry) {
        this.industry = industry;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    /* toString, equals, and hashCode */

    /**
     * Returns a json style <code>String</code> representation of this user
     * @return a json string representing this user
     */
    public String toJsonString() {
        return "{\"name\":\""        + name     + "\""+
                ",\"job_title\":\""   + jobTitle + "\""+
                ",\"industry\":\""    + industry + "\""+
                ",\"version\":"     + version  + "}";
    }

    /**
     * Compares this object to the specified object for equality using name, jobTitle, industry, and version. This
     * implementation is reflexive, symmetric, transitive, self-consistent. It is also consistent with the hashCode
     * implementation.
     * @param o the object to compare with
     * @return <code>true</code> if the objects
     */
    @Override
    public boolean equals(Object o) {
        //check pointer
        if (this == o) return true;
        //check class
        if (o == null || getClass() != o.getClass()) return false;
        //cast and check fields
        User user = (User) o;
        if (name != null ? !name.equals(user.name) : user.name != null) return false;
        if (jobTitle != null ? !jobTitle.equals(user.jobTitle) : user.jobTitle != null) return false;
        if (industry != null ? !industry.equals(user.industry) : user.industry != null) return false;
        if (version != user.version) return false;
        //if all fields are equal, objects are equal
        return true;
    }

    /**
     * Returns a hash code for this <code>User</code> using the <code>name</code>, <code>jobTitle</code>,
     * <code>industry</code>, and <code>version</code> fields.
     * @return
     */
    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (jobTitle != null ? jobTitle.hashCode() : 0);
        result = 31 * result + (industry != null ? industry.hashCode() : 0);
        result = 31 * result + (int) (version ^ (version >>> 32)); //long hashing method mimics java.lang.Long
        return result;
    }
}
