package dev.vality.analytics.utils;

import dev.vality.damsel.analytics.SubError;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SubErrorGeneratorTest {

    @Test
    public void generateError() {
        SubError subError = SubErrorGenerator.generateError("authorization_failed:rejected_by_issuer");

        Assertions.assertEquals(subError.code, "authorization_failed");
        Assertions.assertEquals(subError.sub_error.code, "rejected_by_issuer");

        subError = SubErrorGenerator.generateError(null);

        Assertions.assertEquals(subError.code, SubErrorGenerator.EMPTY_ERROR_CODE);
    }
}