package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
public class BalanceController {

    private final UserRepository userRepository;

    @Autowired
    public BalanceController(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam("userId") long userId) {
        Optional<UserRecord> userRecord = Optional.ofNullable(userRepository.findById(userId));

        if (userRecord.isPresent()) {
            // Return the user's current balance rounded down to the nearest integer
            int roundedBalance = (int) Math.floor(userRecord.get().getBalance());
            return new Balance(roundedBalance);
        }

        // Return balance of 0 if user is not found
        return new Balance(0);
    }
}
