package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Service
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    @Autowired
    private RestTemplate restTemplate;

    @Value("${general.kafka-topic}")
    private String topicName;

    public TransactionListener(UserRepository userRepository, TransactionRepository transactionRepository, RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    @Transactional
    public void listen(ConsumerRecord<String, Transaction> record) {
        Transaction transaction = record.value();
        System.out.println("Received transaction: " + transaction);

        Optional<UserRecord> senderOpt = Optional.ofNullable(userRepository.findById(transaction.getSenderId()));
        Optional<UserRecord> recipientOpt = Optional.ofNullable(userRepository.findById(transaction.getRecipientId()));

        if (senderOpt.isPresent() && recipientOpt.isPresent()) {
            UserRecord sender = senderOpt.get();
            UserRecord recipient = recipientOpt.get();

            if (sender.getBalance() >= transaction.getAmount()) {
                // Deduct amount from sender's balance
                sender.setBalance(sender.getBalance() - transaction.getAmount());
                userRepository.save(sender);

                System.out.println("Sending transaction to Incentive API: " + transaction);

                // Call Incentive API
                String incentiveApiUrl = "http://localhost:8080/incentive";
                Incentive incentive = restTemplate.postForObject(incentiveApiUrl, transaction, Incentive.class);

                if (incentive == null) {
                    System.out.println("Incentive API returned null!");
                } else {
                    System.out.println("Incentive API returned: " + incentive.getAmount());
                }

                float incentiveAmount = incentive != null ? incentive.getAmount() : 0;

                System.out.println("Incentive API Response: " + incentive);

                // Add only incentive amount to recipient's balance
                recipient.setBalance(recipient.getBalance() + incentiveAmount);
                transaction.setIncentive(incentiveAmount);
                userRepository.save(recipient);

                // Save transaction record with incentive
                TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
                transactionRepository.save(transactionRecord);

                // Print the updated balance of the recipient
                int roundedBalance = (int) Math.floor(recipient.getBalance());
                System.out.println("Updated balance for recipient " + recipient.getName() + ": " + roundedBalance);

                // Display all user balances after processing the transaction
                showUserBalances();
            } else {
                System.out.println("Transaction declined: Insufficient funds.");
            }
        } else {
            System.out.println("Transaction declined: Invalid sender or recipient.");
        }
    }

    public void showUserBalances() {
        System.out.println("\n=== User Balances After All Transactions ===");
        userRepository.findAll().forEach(user -> {
            int roundedBalance = (int) Math.floor(user.getBalance());  // Round down to nearest integer
            System.out.println("User: " + user.getName() + ", Balance: " + roundedBalance);
        });
        System.out.println("==========================================\n");
    }

}

class Incentive {
    private float amount;

    public float getAmount() {
        return amount;
    }

    public void setAmount(float amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Incentive{amount=" + amount + "}";
    }
}
