package org.analyzr.controller;

import org.analyzr.domain.DbTable;
import org.analyzr.exception.StorageException;
import org.analyzr.service.StorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

/**
 * @author naimulhuda
 * @since 3/10/2016
 */
@RestController
public class StorageController {
    @Autowired
    private StorageService storageService;

    @RequestMapping(value = "/upload", method = RequestMethod.POST)
    public String uploadFile(@RequestParam("file") MultipartFile file) {
        DbTable table = storageService.processUploadedFile(file);
        return String.format("Table '%s' created with data from file with %d records",
                table.getTableName(), storageService.count(table)) ;
    }

    @ExceptionHandler(StorageException.class)
    public ResponseEntity handleStorageFileNotFound(StorageException e) {
        return ResponseEntity.badRequest()
                .body(e.getMessage());
    }

}
