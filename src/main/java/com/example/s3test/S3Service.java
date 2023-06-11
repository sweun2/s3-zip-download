package com.example.s3test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferProgress;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.ZipFile;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;

import static org.apache.catalina.security.SecurityUtil.remove;

@Service
@RequiredArgsConstructor
@Slf4j
public class S3Service {

    private final AmazonS3 amazonS3;
    private final TransferManager transferManager;

    @Value("${aws.s3.bucket}")
    private String bucket;

    public Resource downloadZip1(String prefix) throws IOException, InterruptedException {

        // (1)
        // 서버 로컬에 생성되는 디렉토리, 해당 디렉토리에 파일이 다운로드된다
        File localDirectory = new File(RandomStringUtils.randomAlphanumeric(6) + "-s3-download");
        // 서버 로컬에 생성되는 zip 파일
        ZipFile zipFile = new ZipFile(RandomStringUtils.randomAlphanumeric(6) + "-s3-download.zip");
        try {
            // (2)
            // TransferManager -> localDirectory에 파일 다운로드
            MultipleFileDownload downloadDirectory = transferManager.downloadDirectory(bucket, prefix, localDirectory);

            // (3)
            // 다운로드 상태 확인
            log.info("[" + prefix + "] download progressing... start");
            DecimalFormat decimalFormat = new DecimalFormat("##0.00");
            while (!downloadDirectory.isDone()) {
                Thread.sleep(1000);
                TransferProgress progress = downloadDirectory.getProgress();
                double percentTransferred = progress.getPercentTransferred();
                log.info("[" + prefix + "] " + decimalFormat.format(percentTransferred) + "% download progressing...");
            }
            log.info("[" + prefix + "] download directory from S3 success!");

            // (4)
            // 로컬 디렉토리 -> 로컬 zip 파일에 압축
            log.info("compressing to zip file...");
            zipFile.addFolder(new File(localDirectory.getName() + "/" + prefix));
        } finally {
            // (5)
            // 로컬 디렉토리 삭제
            File[] files = localDirectory.listFiles();
            assert files != null;
            for (File file : files) {
                remove(file);
            }
            if (localDirectory.delete()) {
                log.info("File [" + localDirectory.getName() + "] delete success");
            }
        }

        // (6)
        // 파일 Resource 리턴
        return new FileSystemResource(zipFile.getFile().getName());
    }
}
