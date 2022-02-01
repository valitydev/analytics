package dev.vality.analytics.service;

import dev.vality.dao.DaoException;

public interface DomainObjectDao<T> {

    Long save(T domainObject) throws DaoException;

}
