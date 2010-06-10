package com.goodworkalan.pack.mix;

import com.goodworkalan.mix.ProjectModule;
import com.goodworkalan.mix.builder.Builder;
import com.goodworkalan.mix.cookbook.JavaProject;

/**
 * Builds the project definition for Pack.
 *
 * @author Alan Gutierrez
 */
public class PackProject implements ProjectModule {
    /**
     * Build the project definition for Pack.
     *
     * @param builder
     *          The project builder.
     */
    public void build(Builder builder) {
        builder
            .cookbook(JavaProject.class)
                .produces("com.github.bigeasy.pack/pack/0.1")
                .depends()
                    .production("com.github.bigeasy.region/region/0.1")
                    .production("com.github.bigeasy.sheaf/sheaf/0.1")
                    .production("com.github.bigeasy.vetiver/vetiver/0.1")
                    .development("org.mockito/mockito-core/1.6")
                    .development("args4j/args4j/2.0.8")
                    .development("org.testng/testng-jdk15/5.10")
                    .end()
                .end()
            .end();
    }
}
