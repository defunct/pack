package com.goodworkalan.pack.mix;

import com.goodworkalan.mix.ProjectModule;
import com.goodworkalan.mix.builder.Builder;
import com.goodworkalan.mix.builder.JavaProject;

public class PackProject extends ProjectModule {
    @Override
    public void build(Builder builder) {
        builder
            .cookbook(JavaProject.class)
                .produces("com.github.bigeasy.pack/pack/0.1")
                .main()
                    .depends()
                        .include("com.github.bigeasy.region/region/0.1")
                        .include("com.github.bigeasy.sheaf/sheaf/0.1")
                        .include("com.github.bigeasy.vetiver/vetiver/0.1")
                        .end()
                    .end()
                .test()
                    .depends()
                        .include("org.mockito/mockito-core/1.6")
                        .include("args4j/args4j/2.0.8")
                        .include("org.testng/testng-jdk15/5.10")
                        .end()
                    .end()
                .end()
            .end();
    }
}
