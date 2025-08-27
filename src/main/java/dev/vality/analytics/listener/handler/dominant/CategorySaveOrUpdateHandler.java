package dev.vality.analytics.listener.handler.dominant;

import dev.vality.analytics.dao.repository.postgres.party.management.CategoryDao;
import dev.vality.analytics.domain.db.tables.pojos.Category;
import dev.vality.analytics.listener.handler.dominant.common.AbstractDominantHandler;
import dev.vality.damsel.domain.CategoryObject;
import dev.vality.damsel.domain.DomainObject;
import dev.vality.damsel.domain_config_v2.FinalOperation;
import dev.vality.damsel.domain_config_v2.HistoricalCommit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class CategorySaveOrUpdateHandler extends AbstractDominantHandler.SaveOrUpdateHandler {

    private final CategoryDao categoryDao;

    @Override
    @Transactional
    public void handle(FinalOperation operation, HistoricalCommit historicalCommit) {
        var category = extract(operation).getCategory();
        if (operation.isSetInsert()) {
            log.info(
                    "Save category operation. id='{}' version='{}'",
                    category.getRef().getId(), historicalCommit.getVersion()
            );
            categoryDao.saveCategory(convertToDatabaseObject(category, historicalCommit));
        } else if (operation.isSetUpdate()) {
            log.info(
                    "Update category operation. id='{}' version='{}'",
                    category.getRef().getId(), historicalCommit.getVersion()
            );
            categoryDao.updateCategory(convertToDatabaseObject(category, historicalCommit));
        }
    }

    @Override
    public boolean isHandle(FinalOperation change) {
        return matches(change, DomainObject::isSetCategory);
    }

    private Category convertToDatabaseObject(CategoryObject categoryObject, HistoricalCommit historicalCommit) {
        var category = new Category();
        category.setVersionId(historicalCommit.getVersion());
        category.setCategoryId(categoryObject.getRef().getId());
        dev.vality.damsel.domain.Category data = categoryObject.getData();
        category.setName(data.getName());
        category.setDescription(data.getDescription());
        if (data.isSetType()) {
            category.setType(data.getType().name());
        }
        var changedBy = historicalCommit.getChangedBy();
        category.setChangedById(changedBy.getId());
        category.setChangedByName(changedBy.getName());
        category.setChangedByEmail(changedBy.getEmail());
        return category;
    }
}
