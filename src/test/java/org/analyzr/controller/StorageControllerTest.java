package org.analyzr.controller;


import org.analyzr.config.DaoConfig;
import org.analyzr.config.RootConfig;
import org.analyzr.config.ServletConfig;
import org.analyzr.domain.DbTable;
import org.analyzr.service.StorageService;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.fileUpload;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

/**
 * @author nhuda
 * @since 29/09/16
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(classes = {
        RootConfig.class,
        DaoConfig.class,
        ServletConfig.class})
public class StorageControllerTest {

    @Autowired
    private StorageService storageService;

    @Autowired
    private ResourceLoader resourceLoader;

    private MockMvc mockMvc;

    @Autowired
    private WebApplicationContext wac;

    @Before
    public void setup() throws Exception {
        this.mockMvc = webAppContextSetup(this.wac).build();
    }

    @Test
    public void testUpload() throws Exception {
        Resource resource = resourceLoader.getResource("classpath:Sample Table Data.csv");
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "Sample Table Data.csv",
                "application/csv",
                FileUtils.readFileToByteArray(resource.getFile()));

        this.mockMvc.perform(fileUpload("/upload").file(file))
                .andExpect(status().isOk())
                .andExpect(content().string("Table 'sample_table_data' created with data from file with 2 records"));
    }

    @After
    public void tearDown() throws Exception {
        storageService.drop(new DbTable("sample_table_data"));
    }
}
