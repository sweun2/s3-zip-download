package com.example.s3test;

import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.IOException;

@Controller
@RequiredArgsConstructor
@RequestMapping("/s3")
public class S3Controller {

    private final S3Service s3Service;

    @GetMapping(value = "/download/zip1/**", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<Resource> downloadZip1(HttpServletRequest request) throws IOException, InterruptedException {

        // (1)
        String prefix = getPrefix(request.getRequestURI(), "/s3/download/zip1/");

        // (2)
        Resource resource = s3Service.downloadZip1(prefix);

        // (3)
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Disposition", "attachment; filename=" + new String(resource.getFilename().getBytes("UTF-8"), "ISO-8859-1"));
        return new ResponseEntity<>(resource, headers, HttpStatus.OK);
    }
    @GetMapping(value = "/", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public String hello(HttpServletRequest request) throws IOException, InterruptedException {

        return "hello";
    }

    private String getPrefix(String uri, String regex) {
        String[] split = uri.split(regex);
        return split.length < 2 ? "" : split[1];
    }
}
