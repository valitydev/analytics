package dev.vality.analytics.converter;

import dev.vality.damsel.domain.Contractor;
import dev.vality.damsel.domain.InternationalLegalEntity;
import dev.vality.damsel.domain.RussianLegalEntity;
import dev.vality.damsel.domain.RussianPrivateEntity;
import dev.vality.geck.common.util.TBaseUtil;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
public class ContractorToCurrentContractorConverter implements Converter<Contractor,
        dev.vality.analytics.domain.db.tables.pojos.Contractor> {

    @Override
    public dev.vality.analytics.domain.db.tables.pojos.Contractor convert(Contractor contractor) {
        dev.vality.analytics.domain.db.tables.pojos.Contractor currentContractor =
                new dev.vality.analytics.domain.db.tables.pojos.Contractor();
        currentContractor.setContractorType(TBaseUtil.unionFieldToEnum(contractor,
                dev.vality.analytics.domain.db.enums.ContractorType.class));
        if (contractor.isSetRegisteredUser()) {
            currentContractor.setRegUserEmail(contractor.getRegisteredUser().getEmail());
        } else if (contractor.isSetLegalEntity()) {
            currentContractor.setLegalEntityType(TBaseUtil.unionFieldToEnum(contractor.getLegalEntity(),
                    dev.vality.analytics.domain.db.enums.LegalEntity.class));
            if (contractor.getLegalEntity().isSetRussianLegalEntity()) {
                RussianLegalEntity russianLegalEntity = contractor.getLegalEntity().getRussianLegalEntity();
                currentContractor.setRussianLegalEntityName(russianLegalEntity.getRegisteredName());
                currentContractor.setRussianLegalEntityRegisteredNumber(russianLegalEntity.getRegisteredNumber());
                currentContractor.setRussianLegalEntityInn(russianLegalEntity.getInn());
                currentContractor.setRussianLegalEntityActualAddress(russianLegalEntity.getActualAddress());
                currentContractor.setRussianLegalEntityPostAddress(russianLegalEntity.getPostAddress());
                currentContractor.setRussianLegalEntityRepresentativePosition(
                        russianLegalEntity.getRepresentativePosition());
                currentContractor.setRussianLegalEntityRepresentativeFullName(
                        russianLegalEntity.getRepresentativeFullName());
                currentContractor.setRussianLegalEntityRepresentativeDocument(
                        russianLegalEntity.getRepresentativeDocument());
                currentContractor.setRussianLegalEntityBankAccount(
                        russianLegalEntity.getRussianBankAccount().getAccount());
                currentContractor.setRussianLegalEntityBankName(
                        russianLegalEntity.getRussianBankAccount().getBankName());
                currentContractor.setRussianLegalEntityBankPostAccount(
                        russianLegalEntity.getRussianBankAccount().getBankPostAccount());
                currentContractor.setRussianLegalEntityBankBik(russianLegalEntity.getRussianBankAccount().getBankBik());
            } else if (contractor.getLegalEntity().isSetInternationalLegalEntity()) {
                InternationalLegalEntity internationalLegalEntity =
                        contractor.getLegalEntity().getInternationalLegalEntity();
                currentContractor.setInternationalLegalEntityName(internationalLegalEntity.getLegalName());
                currentContractor.setInternationalLegalEntityTradingName(internationalLegalEntity.getTradingName());
                currentContractor.setInternationalLegalEntityRegisteredAddress(
                        internationalLegalEntity.getRegisteredAddress());
                currentContractor.setInternationalActualAddress(internationalLegalEntity.getActualAddress());
                currentContractor.setInternationalLegalEntityRegisteredNumber(
                        internationalLegalEntity.getRegisteredNumber());
                if (internationalLegalEntity.isSetCountry()) {
                    currentContractor.setInternationalLegalEntityCountryCode(
                            internationalLegalEntity.getCountry().getId().name());
                }
            }
        } else if (contractor.isSetPrivateEntity()) {
            currentContractor.setPrivateEntityType(TBaseUtil.unionFieldToEnum(contractor.getPrivateEntity(),
                    dev.vality.analytics.domain.db.enums.PrivateEntity.class)
            );
            if (contractor.getPrivateEntity().isSetRussianPrivateEntity()) {
                RussianPrivateEntity russianPrivateEntity = contractor.getPrivateEntity().getRussianPrivateEntity();
                if (russianPrivateEntity.getContactInfo() != null) {
                    currentContractor.setRussianPrivateEntityEmail(russianPrivateEntity.getContactInfo().getEmail());
                    currentContractor.setRussianPrivateEntityPhoneNumber(russianPrivateEntity
                            .getContactInfo().getPhoneNumber());
                }
                currentContractor.setRussianPrivateEntityFirstName(russianPrivateEntity.getFirstName());
                currentContractor.setRussianPrivateEntitySecondName(russianPrivateEntity.getSecondName());
                currentContractor.setRussianPrivateEntityMiddleName(russianPrivateEntity.getMiddleName());
            }
        }

        return currentContractor;
    }

}
