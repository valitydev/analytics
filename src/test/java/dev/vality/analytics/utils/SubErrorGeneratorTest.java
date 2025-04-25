package dev.vality.analytics.utils;

import dev.vality.damsel.analytics.SubError;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SubErrorGeneratorTest {

    @Test
    public void generateError() {
        SubError subError = SubErrorGenerator.generateError("authorization_failed:rejected_by_issuer");

        assertEquals(subError.code, "authorization_failed");
        assertEquals(subError.sub_error.code, "rejected_by_issuer");

        subError = SubErrorGenerator.generateError(null);

        assertEquals(subError.code, SubErrorGenerator.EMPTY_ERROR_CODE);
    }
}