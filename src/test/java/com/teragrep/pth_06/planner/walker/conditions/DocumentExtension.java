package com.teragrep.pth_06.planner.walker.conditions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Provides Document object for tests
 */
public final class DocumentExtension implements ParameterResolver {
    final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return parameterContext.getParameter().getType() == Document.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return Assertions.assertDoesNotThrow(() -> factory.newDocumentBuilder().newDocument());
    }
}
