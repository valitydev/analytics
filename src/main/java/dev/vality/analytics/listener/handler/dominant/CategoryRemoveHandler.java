package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.CategoryDao;
import dev.vality.analytics.domain.db.tables.pojos.Category;
import dev.vality.analytics.listener.handler.dominant.common.AbstractDominantHandler;
import dev.vality.damsel.domain.CategoryRef;
import dev.vality.damsel.domain.Reference;
import dev.vality.damsel.domain_config_v2.Author;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class CategoryRemoveHandler extends AbstractDominantHandler.RemoveHandler {

    private final CategoryDao categoryDao;

    @Override
    @Transactional
    public void handle(FinalOperation operation, Author changedBy, long versionId) {
        var categoryRef = extract(operation).getCategory();
        log.info("Remove category operation. id='{}' version='{}'", categoryRef.getId(), versionId);
        categoryDao.removeCategory(convertToDatabaseObject(categoryRef, changedBy, versionId));
    }

    @Override
    public boolean isHandle(FinalOperation change) {
        return matches(change, Reference::isSetCategory);
    }

    private Category convertToDatabaseObject(CategoryRef categoryRef, Author changedBy, long versionId) {
        var category = new Category();
        category.setVersionId(versionId);
        category.setCategoryId(categoryRef.getId());
        category.setChangedById(changedBy.getId());
        category.setChangedByName(changedBy.getName());
        category.setChangedByEmail(changedBy.getEmail());
        return category;
    }
}
