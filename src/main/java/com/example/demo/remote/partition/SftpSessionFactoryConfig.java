package com.example.demo.remote.partition;

import org.apache.sshd.sftp.client.SftpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.file.remote.RemoteFileTemplate;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;

@Configuration
public class SftpSessionFactoryConfig {

    @Bean
    public SessionFactory<SftpClient.DirEntry> sftpSessionFactory() {
        DefaultSftpSessionFactory factory = new DefaultSftpSessionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(22);
        factory.setUser("dusrxcabr");
        factory.setPassword("dusrxcabr");
        factory.setAllowUnknownKeys(true);
        return new CachingSessionFactory<>(factory);
    }

    @Bean
    public RemoteFileTemplate<SftpClient.DirEntry> remoteFileTemplate() {
        return new RemoteFileTemplate<>(sftpSessionFactory());
    }

}
