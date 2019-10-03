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

package org.axonframework.extension.kafka.example.api

import org.axonframework.modelling.command.TargetAggregateIdentifier
import javax.validation.constraints.Min

/**
 * Create account.
 */
data class CreateBankAccountCommand(
        @TargetAggregateIdentifier
        val bankAccountId: String,
        @Min(value = 0, message = "Overdraft limit must not be less than zero")
        val overdraftLimit: Long
)

/**
 * Deposit money.
 */
data class DepositMoneyCommand(
        @TargetAggregateIdentifier
        val bankAccountId: String,
        val amountOfMoney: Long
)

/**
 * Withdraw money.
 */
data class WithdrawMoneyCommand(
        @TargetAggregateIdentifier
        val bankAccountId: String,
        val amountOfMoney: Long
)

/**
 * Return money if transfer is not possible.
 */
data class ReturnMoneyOfFailedBankTransferCommand(
        @TargetAggregateIdentifier
        val bankAccountId: String,
        val amount: Long
)