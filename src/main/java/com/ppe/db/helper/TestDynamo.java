package com.ppe.db.helper;

public class TestDynamo {

    public static void main(String arg[]) {

        FakePicture pix = new FakePicture();
        pix.setFrontView("http://example.com/products/123_front.jpg");
        pix.setRearView("http://example.com/products/123_rear.jpg");
        pix.setSideView("http://example.com/products/123_left_side.jpg");


        FakeCustomer mc1 = new FakeCustomer();
        mc1.setPARTITION_KEY("UUU");
        mc1.setSORT_KEY("1");
        mc1.setFees(200);
        mc1.setFakePicture(pix);


        FakeCustomer mc2 = new FakeCustomer();
        mc2.setPARTITION_KEY("LLL");
        mc2.setSORT_KEY("1");
        mc2.setFees(100);
        mc2.setFakePicture(pix);

        

        EHelper.TxnPacket packet = EHelper.TxnPacket.builder()
                .update(mc1, FakeCustomer.class, null )
//                .update(item1, FakeCustomer.class,null)
//                .insert(mc1,FakeCustomer.class)
//                .insert(mc2, FakeCustomer.class)
                .build();


        EHelper.executeTransactionWrite(packet);

        FakeCustomer item = EHelper.getItem(mc1);
        FakeCustomer item1 = EHelper.getItem(mc2);
        System.out.println(" ITEM  "+ item);
        System.out.println(" ITEM  1 "+ item1);


    }


}
