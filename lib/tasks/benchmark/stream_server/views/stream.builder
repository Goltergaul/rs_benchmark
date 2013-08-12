xml.instruct! :xml, :version => "1.0"
xml.feed :xmlns => "http://www.w3.org/2005/Atom" do

  xml.author do
    xml.name "WorkloadInducer"
  end

  xml.title "WorkloadInducer Stream #{locals[:id]}"

  for post in locals[:articles]
    xml.entry do
      xml.title post[:title]
      xml.content do
        xml.cdata! post[:body]
      end
      xml.updated post[:pub_date]
      xml.link :href => post[:link]
    end
  end
end