package com.tn.service.data.jdbc.repository;

import java.util.Collection;

import com.tn.service.data.jdbc.domain.Field;
import com.tn.service.data.repository.FindException;

public interface FieldRepository
{
  Collection<Field> findForTable(String schema, String table) throws FindException;
}
