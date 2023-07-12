package com.example.demo.remote.partition;

import org.apache.sshd.sftp.client.SftpClient.DirEntry;
import org.springframework.core.io.InputStreamResource;
import org.springframework.integration.file.remote.RemoteFileTemplate;

public class SftpInputStreamResource extends InputStreamResource {

    public SftpInputStreamResource(RemoteFileTemplate<DirEntry> remoteFileTemplate, String remoteFilePath) {
        super(remoteFileTemplate.execute(session -> session.readRaw(remoteFilePath)));
    }

}
