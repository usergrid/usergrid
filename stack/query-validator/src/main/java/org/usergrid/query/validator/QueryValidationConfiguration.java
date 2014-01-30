package org.apache.usergrid.query.validator;

import org.apache.usergrid.persistence.Entity;
import org.apache.usergrid.utils.InflectionUtils;

import java.util.List;

public class QueryValidationConfiguration {

    String org;
    String app;
    String endpointUri;
    String email;
    String password;
    String collection;
    List<Entity> entities;

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        String pluralizeName = InflectionUtils.pluralize(collection);
        this.collection = pluralizeName;
    }

    public String getOrg() {
        return org;
    }

    public void setOrg(String org) {
        this.org = org;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getEndpointUri() {
        return endpointUri;
    }

    public void setEndpointUri(String endpointUri) {
        this.endpointUri = endpointUri;
    }

    public List<Entity> getEntities() {
        return entities;
    }

    public void setEntities(List<Entity> entities) {
        this.entities = entities;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
