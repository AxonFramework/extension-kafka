/*
 * Copyright (c) 2019. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extension.kafka.example.core

import org.axonframework.commandhandling.CommandHandler
import org.axonframework.eventsourcing.EventSourcingHandler
import org.axonframework.extension.kafka.example.api.*
import org.axonframework.modelling.command.AggregateIdentifier
import org.axonframework.modelling.command.AggregateLifecycle.apply
import org.axonframework.spring.stereotype.Aggregate

/**
 * Represent account.
 */
@Suppress("unused")
@Aggregate
class BankAccount() {

    @AggregateIdentifier
    private lateinit var id: String
    private var overdraftLimit: Long = 0
    private var balanceInCents: Long = 0

    /**
     * Creates a new bank account.
     */
    @CommandHandler
    constructor(command: CreateBankAccountCommand) : this() {
        apply(BankAccountCreatedEvent(command.bankAccountId, command.overdraftLimit))
    }

    /**
     * Deposits money to account.
     */
    @CommandHandler
    fun deposit(command: DepositMoneyCommand) {
        apply(MoneyDepositedEvent(id, command.amountOfMoney))
    }

    /**
     * Withdraw money from account.
     */
    @CommandHandler
    fun withdraw(command: WithdrawMoneyCommand) {
        if (command.amountOfMoney <= balanceInCents + overdraftLimit) {
            apply(MoneyWithdrawnEvent(id, command.amountOfMoney))
        }
    }

    /**
     * Return money from account.
     */
    @CommandHandler
    fun returnMoney(command: ReturnMoneyOfFailedBankTransferCommand) {
        apply(MoneyOfFailedBankTransferReturnedEvent(id, command.amount))
    }

    /**
     * Handler to initialize bank accounts attributes.
     */
    @EventSourcingHandler
    fun on(event: BankAccountCreatedEvent) {
        id = event.id
        overdraftLimit = event.overdraftLimit
        balanceInCents = 0
    }

    /**
     * Handler adjusting balance.
     */
    @EventSourcingHandler
    fun on(event: MoneyAddedEvent) {
        balanceInCents += event.amount
    }

    /**
     * Handler adjusting balance.
     */
    @EventSourcingHandler
    fun on(event: MoneySubtractedEvent) {
        balanceInCents -= event.amount
    }
}