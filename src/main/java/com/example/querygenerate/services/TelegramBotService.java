package com.example.querygenerate.services;

import com.pengrad.telegrambot.TelegramBot;
import com.pengrad.telegrambot.request.SendMessage;
import lombok.Value;
import org.springframework.stereotype.Service;

/**
 * @author QuangNN
 */
@Service
public class TelegramBotService {
    private final TelegramBot telegramBot=new TelegramBot("6633422708:AAGRuXo9UjAftB3Kq0FCjuqMz1Kp82CV0NE");
    private final long chatId=-4051019953L; // Chat ID của cuộc trò chuyện với bot

    public void sendLogMessage(String message) {
        telegramBot.execute(new SendMessage(chatId, message));
    }
}
