/*
 * Copyright (c) 2010-2021. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.kafka.example.client

import mu.KLogging
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.extensions.kafka.example.api.CreateBankAccountCommand
import org.axonframework.extensions.kafka.example.api.DepositMoneyCommand
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.*
import java.util.concurrent.CompletableFuture

/**
 * Bank client sending scheduled commands.
 */
@Component
class BankClient(private val commandGateway: CommandGateway) {

    companion object : KLogging()

    private val accountId = UUID.randomUUID().toString()
    private var amount = 100

    /**
     * Creates account once.
     */
    @Scheduled(initialDelay = 5_000, fixedDelay = 1000_000_000)
    fun createAccount() {
        logger.debug { "creating account $accountId" }
        commandGateway.send<CompletableFuture<String>>(CreateBankAccountCommand(bankAccountId = accountId, overdraftLimit = 1000))
    }

    /**
     * Deposit some money every 20 seconds.
     */
    @Scheduled(initialDelay = 10_000, fixedDelay = 20_000)
    fun deposit() {
        logger.debug { "depositing $amount from account $accountId" }
        commandGateway.send<CompletableFuture<String>>(DepositMoneyCommand(bankAccountId = accountId, amountOfMoney = amount.toLong()))
        amount = amount.inc()
    }
}