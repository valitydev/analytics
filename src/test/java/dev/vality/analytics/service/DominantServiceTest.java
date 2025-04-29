package dev.vality.analytics.service;

import dev.vality.analytics.config.PostgresqlTest;
import dev.vality.analytics.dao.repository.postgres.party.management.DominantDao;
import dev.vality.analytics.utils.TestData;
import dev.vality.damsel.domain.CategoryType;
import dev.vality.damsel.domain_config.Commit;
import dev.vality.damsel.domain_config.RepositorySrv;
import dev.vality.testcontainers.annotations.DefaultSpringBootTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

@DefaultSpringBootTest
@PostgresqlTest
public class DominantServiceTest {

    @MockitoBean
    private RepositorySrv.Iface dominantClient;
    @Autowired
    private DominantService dominantService;
    @Autowired
    private DominantDao dominantDao;

    @BeforeEach
    public void setUp() throws Exception {
        Commit firstCommit = TestData.buildInsertCategoryCommit(64, "testName", "testDescription", CategoryType.test);
        Commit secondCommit =
                TestData.buildUpdateCategoryCommit(64, "testNameNew", "testDescriptionNew", CategoryType.live,
                        firstCommit.getOps().get(0).getInsert().getObject());
        Map<Long, Commit> commitMap = new HashMap<>();
        commitMap.put(1L, firstCommit);
        commitMap.put(2L, secondCommit);
        when(dominantClient.pullRange(anyLong(), anyInt())).thenReturn(commitMap);
    }

    @Test
    public void testDominantVersion() {
        dominantService.pullDominantRange(10);
        Assertions.assertEquals(2, (long) dominantDao.getLastVersion());
    }
}
